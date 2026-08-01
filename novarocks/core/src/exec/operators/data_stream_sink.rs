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
use crate::exec::fragment::sink::{DataStreamPartitionType, DataStreamSinkFactoryInput};
use crate::runtime::endpoint::FragmentDestination;
use crate::runtime::exchange;
use crate::runtime::fragment::io::exchange::{ExchangeFrame, ExchangeFrameTransmitter};
use crate::runtime::fragment::io::exchange_queue::{
    ExchangeSendTask, ExchangeSendTracker, exchange_send_queue,
};
use crate::runtime::mem_tracker::{MemTracker, TrackedBytes};
use arrow::datatypes::DataType;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
#[cfg(test)]
use std::sync::{Mutex, OnceLock};

use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::profile::{ProfileUnit, clamp_u128_to_i64};
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};

const NEED_INPUT_LOG_EVERY: u64 = 1;

static NEED_INPUT_BLOCKED_LOG_COUNT: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
static DATA_STREAM_PAYLOAD_IDENTITIES: OnceLock<Mutex<Vec<(UniqueId, i32, bool)>>> =
    OnceLock::new();

#[cfg(test)]
pub(crate) fn take_eos_be_number_for_test(fragment_instance_id: UniqueId) -> Option<i32> {
    let mut identities = DATA_STREAM_PAYLOAD_IDENTITIES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .expect("data stream payload identity lock");
    let position = identities
        .iter()
        .position(|(finst_id, _, eos)| *finst_id == fragment_instance_id && *eos)?;
    Some(identities.swap_remove(position).1)
}

#[cfg(test)]
fn record_payload_identity_for_test(fragment_instance_id: UniqueId, be_number: i32, eos: bool) {
    DATA_STREAM_PAYLOAD_IDENTITIES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .expect("data stream payload identity lock")
        .push((fragment_instance_id, be_number, eos));
}

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

fn is_low_cardinality_exchange_dictionary(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Dictionary(key_type, value_type)
            if key_type.as_ref() == &DataType::Int32
                && matches!(value_type.as_ref(), DataType::Utf8 | DataType::LargeUtf8)
    )
}

// Hash partition implementation (vectorized, no row conversion)
mod data_stream_sink_hash_partition {
    use super::{Chunk, ExprArena, ExprId, is_low_cardinality_exchange_dictionary};
    use arrow::array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
        DictionaryArray, FixedSizeBinaryArray, Float32Array, Float64Array, Int16Array, Int32Array,
        Int64Array, LargeBinaryArray, LargeStringArray, ListArray, StringArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray,
    };
    use arrow::compute::cast;
    use arrow::compute::take;
    use arrow::datatypes::{DataType, Int32Type, TimeUnit};
    use std::sync::Arc;

    use crate::exec::hash_table::key_builder::encode_group_key_row;
    use novarocks_types::largeint;

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

    fn dictionary_int32_indices(array: &ArrayRef) -> Result<&DictionaryArray<Int32Type>, String> {
        array
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .ok_or_else(|| {
                format!(
                    "hash_partition: failed to downcast dictionary carrier {:?} to DictionaryArray<Int32Type>",
                    array.data_type()
                )
            })
    }

    fn dictionary_key_index(key: i32, values_len: usize, row: usize) -> Result<usize, String> {
        let idx = usize::try_from(key)
            .map_err(|_| format!("hash_partition: negative dictionary key {key} at row {row}"))?;
        if idx >= values_len {
            return Err(format!(
                "hash_partition: dictionary key {key} at row {row} exceeds values len {values_len}"
            ));
        }
        Ok(idx)
    }

    fn compute_fnv_hash_dictionary_array(array: &ArrayRef) -> Result<Vec<u64>, String> {
        let dict = dictionary_int32_indices(array)?;
        let values = dict.values();
        let value_hashes = compute_fnv_hash_array(values)?;
        let keys = dict.keys();
        let mut out = vec![FNV_SEED; dict.len()];
        for (row, hash_value) in out.iter_mut().enumerate().take(dict.len()) {
            if dict.is_null(row) {
                *hash_value = hash_value.wrapping_mul(FNV_PRIME);
            } else {
                let idx = dictionary_key_index(keys.value(row), value_hashes.len(), row)?;
                *hash_value = value_hashes[idx];
            }
        }
        Ok(out)
    }

    fn compute_crc32_hash_dictionary_array(array: &ArrayRef) -> Result<Vec<u32>, String> {
        let dict = dictionary_int32_indices(array)?;
        let values = dict.values();
        let value_hashes = compute_crc32_hash_array(values)?;
        let keys = dict.keys();
        let mut out = vec![0u32; dict.len()];
        for (row, hash_value) in out.iter_mut().enumerate().take(dict.len()) {
            if dict.is_null(row) {
                *hash_value = 0;
            } else {
                let idx = dictionary_key_index(keys.value(row), value_hashes.len(), row)?;
                *hash_value = value_hashes[idx];
            }
        }
        Ok(out)
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
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if arr.is_null(i) {
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    } else {
                        let val = if arr.value(i) { 1u8 } else { 0u8 };
                        *hash_value ^= val as u64;
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    }
                }
            }
            DataType::Int64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| "failed to downcast to Int64Array".to_string())?;
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if arr.is_null(i) {
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    } else {
                        let val = arr.value(i);
                        let bytes = val.to_le_bytes();
                        *hash_value ^= fnv_hash_value(&bytes);
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    }
                }
            }
            DataType::Int32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| "failed to downcast to Int32Array".to_string())?;
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if arr.is_null(i) {
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    } else {
                        let val = arr.value(i);
                        let bytes = val.to_le_bytes();
                        *hash_value ^= fnv_hash_value(&bytes);
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    }
                }
            }
            DataType::Int16 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .ok_or_else(|| "failed to downcast to Int16Array".to_string())?;
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if arr.is_null(i) {
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    } else {
                        let val = arr.value(i);
                        let bytes = val.to_le_bytes();
                        *hash_value ^= fnv_hash_value(&bytes);
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    }
                }
            }
            DataType::Float64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| "failed to downcast to Float64Array".to_string())?;
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if arr.is_null(i) {
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    } else {
                        let val = arr.value(i);
                        let bytes = val.to_bits().to_le_bytes();
                        *hash_value ^= fnv_hash_value(&bytes);
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    }
                }
            }
            DataType::Float32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| "failed to downcast to Float32Array".to_string())?;
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if arr.is_null(i) {
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    } else {
                        let val = arr.value(i);
                        let bytes = val.to_bits().to_le_bytes();
                        *hash_value ^= fnv_hash_value(&bytes);
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    }
                }
            }
            DataType::Utf8 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| "failed to downcast to StringArray".to_string())?;
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if arr.is_null(i) {
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    } else {
                        let val = arr.value(i);
                        *hash_value ^= fnv_hash_value(val.as_bytes());
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    }
                }
            }
            DataType::LargeUtf8 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .ok_or_else(|| "failed to downcast to LargeStringArray".to_string())?;
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if arr.is_null(i) {
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    } else {
                        let val = arr.value(i);
                        *hash_value ^= fnv_hash_value(val.as_bytes());
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    }
                }
            }
            DataType::Binary => {
                let arr = array
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| "failed to downcast to BinaryArray".to_string())?;
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if arr.is_null(i) {
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    } else {
                        *hash_value ^= fnv_hash_value(arr.value(i));
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    }
                }
            }
            DataType::LargeBinary => {
                let arr = array
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .ok_or_else(|| "failed to downcast to LargeBinaryArray".to_string())?;
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if arr.is_null(i) {
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    } else {
                        *hash_value ^= fnv_hash_value(arr.value(i));
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    }
                }
            }
            DataType::Date32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .ok_or_else(|| "failed to downcast to Date32Array".to_string())?;
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if arr.is_null(i) {
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    } else {
                        let val = arr.value(i);
                        let bytes = val.to_le_bytes();
                        *hash_value ^= fnv_hash_value(&bytes);
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    }
                }
            }
            DataType::Timestamp(unit, _tz) => {
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if array.is_null(i) {
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    } else {
                        let val = match unit {
                            TimeUnit::Second => {
                                let arr = array
                                    .as_any()
                                    .downcast_ref::<TimestampSecondArray>()
                                    .ok_or_else(|| {
                                        "failed to downcast to TimestampSecondArray".to_string()
                                    })?;
                                arr.value(i)
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
                        *hash_value ^= fnv_hash_value(&bytes);
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    }
                }
            }
            DataType::Decimal128(_, _) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| "failed to downcast to Decimal128Array".to_string())?;
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if arr.is_null(i) {
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    } else {
                        let val = arr.value(i);
                        let bytes = val.to_le_bytes();
                        *hash_value ^= fnv_hash_value(&bytes);
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    }
                }
            }
            DataType::Decimal256(_, _) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Decimal256Array>()
                    .ok_or_else(|| "failed to downcast to Decimal256Array".to_string())?;
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if arr.is_null(i) {
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    } else {
                        let val = arr.value(i);
                        let bytes = val.to_le_bytes();
                        *hash_value ^= fnv_hash_value(&bytes);
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    }
                }
            }
            DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
                let arr = array
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .ok_or_else(|| "failed to downcast to FixedSizeBinaryArray".to_string())?;
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if arr.is_null(i) {
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    } else {
                        let value = largeint::i128_from_be_bytes(arr.value(i)).map_err(|e| {
                            format!("hash_partition: decode LARGEINT failed at row {}: {}", i, e)
                        })?;
                        let bytes = value.to_le_bytes();
                        *hash_value ^= fnv_hash_value(&bytes);
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
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
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if list.is_null(i) {
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    } else {
                        let row_hash = fnv_hash_list_utf8_row(list, values, i);
                        *hash_value ^= row_hash;
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
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
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if list.is_null(i) {
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    } else {
                        let row_hash = fnv_hash_list_int32_row(list, values, i);
                        *hash_value ^= row_hash;
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
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
            dict_type if is_low_cardinality_exchange_dictionary(dict_type) => {
                return compute_fnv_hash_dictionary_array(array);
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
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if !arr.is_null(i) {
                        let val = if arr.value(i) { 1u8 } else { 0u8 };
                        *hash_value = crc32_hash_value(&[val]);
                    }
                }
            }
            DataType::Int64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| "failed to downcast to Int64Array".to_string())?;
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if !arr.is_null(i) {
                        let val = arr.value(i);
                        let bytes = val.to_le_bytes();
                        *hash_value = crc32_hash_value(&bytes);
                    }
                }
            }
            DataType::Int32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| "failed to downcast to Int32Array".to_string())?;
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if !arr.is_null(i) {
                        let val = arr.value(i);
                        let bytes = val.to_le_bytes();
                        *hash_value = crc32_hash_value(&bytes);
                    }
                }
            }
            DataType::Int16 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .ok_or_else(|| "failed to downcast to Int16Array".to_string())?;
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if !arr.is_null(i) {
                        let val = arr.value(i);
                        let bytes = val.to_le_bytes();
                        *hash_value = crc32_hash_value(&bytes);
                    }
                }
            }
            DataType::Float64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| "failed to downcast to Float64Array".to_string())?;
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if !arr.is_null(i) {
                        let val = arr.value(i);
                        let bytes = val.to_bits().to_le_bytes();
                        *hash_value = crc32_hash_value(&bytes);
                    }
                }
            }
            DataType::Float32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| "failed to downcast to Float32Array".to_string())?;
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if !arr.is_null(i) {
                        let val = arr.value(i);
                        let bytes = val.to_bits().to_le_bytes();
                        *hash_value = crc32_hash_value(&bytes);
                    }
                }
            }
            DataType::Utf8 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| "failed to downcast to StringArray".to_string())?;
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if !arr.is_null(i) {
                        let val = arr.value(i);
                        *hash_value = crc32_hash_value(val.as_bytes());
                    }
                }
            }
            DataType::LargeUtf8 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .ok_or_else(|| "failed to downcast to LargeStringArray".to_string())?;
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if !arr.is_null(i) {
                        let val = arr.value(i);
                        *hash_value = crc32_hash_value(val.as_bytes());
                    }
                }
            }
            DataType::Binary => {
                let arr = array
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| "failed to downcast to BinaryArray".to_string())?;
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if !arr.is_null(i) {
                        *hash_value = crc32_hash_value(arr.value(i));
                    }
                }
            }
            DataType::LargeBinary => {
                let arr = array
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .ok_or_else(|| "failed to downcast to LargeBinaryArray".to_string())?;
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if !arr.is_null(i) {
                        *hash_value = crc32_hash_value(arr.value(i));
                    }
                }
            }
            DataType::Date32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .ok_or_else(|| "failed to downcast to Date32Array".to_string())?;
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if !arr.is_null(i) {
                        let val = arr.value(i);
                        let bytes = val.to_le_bytes();
                        *hash_value = crc32_hash_value(&bytes);
                    }
                }
            }
            DataType::Timestamp(unit, _tz) => {
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if !array.is_null(i) {
                        let val = match unit {
                            TimeUnit::Second => {
                                let arr = array
                                    .as_any()
                                    .downcast_ref::<TimestampSecondArray>()
                                    .ok_or_else(|| {
                                        "failed to downcast to TimestampSecondArray".to_string()
                                    })?;
                                arr.value(i)
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
                        *hash_value = crc32_hash_value(&bytes);
                    }
                }
            }
            DataType::Decimal128(_, _) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| "failed to downcast to Decimal128Array".to_string())?;
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if !arr.is_null(i) {
                        let val = arr.value(i);
                        let bytes = val.to_le_bytes();
                        *hash_value = crc32_hash_value(&bytes);
                    }
                }
            }
            DataType::Decimal256(_, _) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Decimal256Array>()
                    .ok_or_else(|| "failed to downcast to Decimal256Array".to_string())?;
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if !arr.is_null(i) {
                        let val = arr.value(i);
                        let bytes = val.to_le_bytes();
                        *hash_value = crc32_hash_value(&bytes);
                    }
                }
            }
            DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
                let arr = array
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .ok_or_else(|| "failed to downcast to FixedSizeBinaryArray".to_string())?;
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if !arr.is_null(i) {
                        let value = largeint::i128_from_be_bytes(arr.value(i)).map_err(|e| {
                            format!("hash_partition: decode LARGEINT failed at row {}: {}", i, e)
                        })?;
                        let bytes = value.to_le_bytes();
                        *hash_value = crc32_hash_value(&bytes);
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
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
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
                    *hash_value = crc32_hash_value(&encoded);
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
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
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
                    *hash_value = crc32_hash_value(&encoded);
                }
            }
            DataType::List(_) | DataType::Struct(_) | DataType::Map(_, _) => {
                for (row, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if let Some(encoded) = encode_group_key_row(array, row)? {
                        *hash_value = crc32_hash_value(&encoded);
                    }
                }
            }
            dict_type if is_low_cardinality_exchange_dictionary(dict_type) => {
                return compute_crc32_hash_dictionary_array(array);
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
        for rows in partition_row_indices.iter().take(num_partitions) {
            if rows.is_empty() {
                partition_chunks.push(Chunk::default());
            } else {
                // Use Arrow take to select rows for this partition
                let indices = arrow::array::UInt32Array::from(rows.clone());
                let indices_ref = Arc::new(indices) as arrow::array::ArrayRef;

                let mut new_columns = Vec::with_capacity(chunk.batch.num_columns());
                for col in chunk.batch.columns() {
                    let taken = take(col.as_ref(), &indices_ref, None)
                        .map_err(|e| format!("Arrow take failed: {}", e))?;
                    new_columns.push(taken);
                }

                let new_chunk = Chunk::try_new_with_columns(chunk.chunk_schema_ref(), new_columns)
                    .map_err(|e| format!("Failed to create partition chunk: {}", e))?;

                partition_chunks.push(new_chunk);
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
    input: DataStreamSinkFactoryInput,
    fragment_instance_id: UniqueId,
    sender_id: Option<i32>,
    partition_arena: ExprArena,
    transmitter: Arc<dyn ExchangeFrameTransmitter>,
    plan_node_id: i32,
    finish_state: Arc<DataStreamSinkFinishState>,
    shared_sequence: Arc<AtomicI64>,
}

impl DataStreamSinkFactory {
    pub(crate) fn new(
        input: DataStreamSinkFactoryInput,
        fragment_instance_id: UniqueId,
        sender_id: Option<i32>,
        plan_node_id: i32,
        partition_arena: ExprArena,
        transmitter: Arc<dyn ExchangeFrameTransmitter>,
    ) -> Self {
        // Align with StarRocks FE ExplainAnalyzer: ExchangeSinkOperator uses the *upstream plan node id*
        // (not the destination exchange node id) as `plan_node_id`.
        let name = if plan_node_id >= 0 {
            format!("EXCHANGE_SINK (id={plan_node_id})")
        } else {
            "EXCHANGE_SINK".to_string()
        };
        let init_error = (!input.output_exprs.is_empty())
            .then(|| "DATA_STREAM_SINK output_exprs are not supported".to_string());

        Self {
            name,
            init_error,
            input,
            fragment_instance_id,
            sender_id,
            partition_arena,
            transmitter,
            plan_node_id,
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
            .sender_id
            .unwrap_or((self.fragment_instance_id.low() as i32) & 0x7fffffff);
        // Initial value only; overwritten by bind_runtime_state /
        // sync_be_number from RuntimeState.backend_num (the FE-assigned instance
        // index) before the sink sends. At instance 0 this stays 0.
        let be_number = 0i32;

        if driver_id == 0 {
            let part_type = self.input.output_partition_type.display_name();
            let dest_count = self.input.destinations.len();
            let dest_preview = self
                .input
                .destinations
                .iter()
                .take(3)
                .map(|d| format_uuid(d.finst_id().high(), d.finst_id().low()))
                .collect::<Vec<_>>()
                .join(",");
            debug!(
                "DataStreamSink created: finst={} plan_node_id={} dest_node_id={} part_type={} dop={} sender_id={} be_number={} destinations={} dest_preview=[{}]",
                format_uuid(
                    self.fragment_instance_id.high(),
                    self.fragment_instance_id.low()
                ),
                self.plan_node_id,
                self.input.dest_node_id,
                part_type,
                dop.max(1),
                sender_id,
                be_number,
                dest_count,
                dest_preview
            );
        }

        let mut init_error = self.init_error.clone();
        let arena = self.partition_arena.clone();
        let expr_ids = self.input.output_partition_exprs.clone();
        if init_error.is_none() && self.input.output_partition_type.requires_exprs() {
            let has_variant = expr_ids.iter().any(|id| {
                arena
                    .data_type(*id)
                    .is_some_and(|dt| matches!(dt, arrow::datatypes::DataType::LargeBinary))
            });
            if has_variant {
                init_error =
                    Some("VARIANT is not supported in HASH_PARTITIONED partition keys".to_string());
            }
        }

        let send_observable = Arc::new(Observable::new());
        exchange_send_queue().register_send_observer(&send_observable);

        Box::new(DataStreamSinkOperator {
            name: self.name.clone(),
            input: self.input.clone(),
            fragment_instance_id: self.fragment_instance_id,
            transmitter: Arc::clone(&self.transmitter),
            arena,
            expr_ids,
            init_error,
            driver_id,
            sender_id,
            be_number,
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
    be_number: i32,
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
    input: DataStreamSinkFactoryInput,
    fragment_instance_id: UniqueId,
    transmitter: Arc<dyn ExchangeFrameTransmitter>,
    arena: ExprArena,
    expr_ids: Vec<ExprId>,
    init_error: Option<String>,
    driver_id: i32,
    sender_id: i32,
    be_number: i32,
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

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> Result<(), String> {
        self.be_number = state.backend_num().unwrap_or(0);
        Ok(())
    }

    fn prepare(&mut self) -> Result<(), String> {
        // Align with StarRocks: count actual sink drivers prepared, not planned DOP.
        self.finish_state.register_driver();
        crate::novarocks_logging::debug!(
            "DataStreamSink registered driver: finst={} driver_id={} dest_node_id={} sender_id={} remaining_drivers={}",
            format_uuid(
                self.fragment_instance_id.high(),
                self.fragment_instance_id.low()
            ),
            self.driver_id,
            self.input.dest_node_id,
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
    fn sync_be_number(&mut self, state: &RuntimeState) {
        if let Some(be_number) = state.backend_num() {
            self.be_number = be_number;
        }
    }

    fn pending_chunk_bytes_total(&self) -> usize {
        self.pending_bytes_per_dest.iter().sum()
    }

    fn pending_chunk_count_total(&self) -> usize {
        self.pending_per_dest.iter().map(VecDeque::len).sum()
    }

    fn has_pending_chunks(&self) -> bool {
        self.pending_per_dest
            .iter()
            .any(|chunks| !chunks.is_empty())
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
        self.has_pending_chunks() || self.has_pending_payloads()
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

    fn should_flush_pending_dest(&self, dest_idx: usize, force: bool) -> bool {
        let Some(pending) = self.pending_per_dest.get(dest_idx) else {
            return false;
        };
        if pending.is_empty() {
            return false;
        }
        if force {
            return true;
        }
        self.pending_bytes_per_dest
            .get(dest_idx)
            .copied()
            .unwrap_or(0)
            >= self.max_transmit_batched_bytes.max(1)
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
            "DataStreamSink need_input blocked: reason={} finst={} driver_id={} sender_id={} pending_chunk_bytes={} pending_chunks={} pending_payloads={} pending_payload_bytes={} reserve_bytes={} inflight_bytes={} max_inflight_bytes={} finishing={} send_idle={} send_inflight_bytes={}",
            reason,
            format_uuid(
                self.fragment_instance_id.high(),
                self.fragment_instance_id.low()
            ),
            self.driver_id,
            self.sender_id,
            self.pending_chunk_bytes_total(),
            self.pending_chunk_count_total(),
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
        let dest_id = self.input.dest_node_id;
        let part_type = self.input.output_partition_type.display_name();
        if let Some(profile) = self.profiles.as_ref() {
            profile
                .common
                .add_info_string("DestID", format!("{dest_id}"));
            profile.common.add_info_string("PartType", part_type);
            profile.common.counter_add(
                "ChannelNum",
                ProfileUnit::Unit,
                clamp_u128_to_i64(channel_num),
            );
        }
    }

    fn destinations(&self) -> &[FragmentDestination] {
        &self.input.destinations
    }

    /// Returns true if the destination at index `i` is a pseudo (pruned-bucket) destination.
    ///
    /// In bucket shuffle joins, FE creates one destination per bucket of the probe-side table.
    /// When predicates prune away all tablets in some buckets, FE still emits destination entries
    /// to keep the bucket index ↔ destination index mapping correct for the hash partitioner,
    /// but marks them with `fragment_instance_id.lo == -1` and an invalid (zero) port.
    /// No fragment instance runs for these buckets, so the sender must skip them entirely:
    /// no data buffering, no serialization, no EOS packet.
    /// StarRocks BE applies the same `lo == -1` check in DataStreamSender::send_chunk(),
    /// ExchangeSinkOperator::push_chunk(), and Channel::close().
    fn is_pseudo_destination(dest: &FragmentDestination) -> bool {
        dest.finst_id().low() == -1
    }

    fn partition_chunk(&mut self, chunk: &Chunk) -> Result<Vec<Vec<Chunk>>, String> {
        // Get destinations first to avoid borrowing self
        let dests: Vec<FragmentDestination> = self.destinations().to_vec();
        if dests.is_empty() {
            return Ok(vec![]);
        }

        if chunk.is_empty() {
            return Ok(vec![Vec::new(); dests.len()]);
        }

        let n = dests.len();

        match self.input.output_partition_type {
            DataStreamPartitionType::Unpartitioned => {
                // Broadcast to all destinations
                let mut per_dest_chunks: Vec<Vec<Chunk>> = Vec::with_capacity(n);
                for _ in 0..n {
                    per_dest_chunks.push(vec![chunk.clone()]);
                }
                Ok(per_dest_chunks)
            }
            DataStreamPartitionType::Random => {
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
                for row_indices in partition_row_indices.iter().take(n) {
                    if row_indices.is_empty() {
                        per_dest_chunks.push(Vec::new());
                    } else {
                        let indices = arrow::array::UInt32Array::from(row_indices.clone());
                        let indices_ref = Arc::new(indices) as arrow::array::ArrayRef;

                        let mut new_columns = Vec::with_capacity(chunk.batch.num_columns());
                        for col in chunk.batch.columns() {
                            let taken = arrow::compute::take(col.as_ref(), &indices_ref, None)
                                .map_err(|e| format!("Arrow take failed: {}", e))?;
                            new_columns.push(taken);
                        }

                        let new_chunk =
                            Chunk::try_new_with_columns(chunk.chunk_schema_ref(), new_columns)
                                .map_err(|e| format!("Failed to create partition chunk: {}", e))?;

                        per_dest_chunks.push(vec![new_chunk]);
                    }
                }
                Ok(per_dest_chunks)
            }
            DataStreamPartitionType::HashPartitioned
            | DataStreamPartitionType::BucketShuffleHashPartitioned => {
                if self.expr_ids.is_empty() {
                    return Err("HASH_PARTITIONED missing partition_exprs".to_string());
                }

                // Use vectorized hash partition without row conversion
                let use_crc32 = matches!(
                    self.input.output_partition_type,
                    DataStreamPartitionType::BucketShuffleHashPartitioned
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
        }
    }

    fn try_enqueue_payload(
        &mut self,
        dest: &FragmentDestination,
        mut pending: PendingPayload,
        allow_overflow: bool,
    ) -> Result<PayloadEnqueue, String> {
        let allow_overflow =
            allow_overflow || pending.payload_bytes > exchange_send_queue().max_inflight_bytes();
        let reserve_bytes = pending.payload_bytes.max(1);

        let addr = dest.endpoint();
        let dest_finst_id = UniqueId::new(dest.finst_id().high(), dest.finst_id().low());
        let error_state = self
            .error_state
            .as_ref()
            .ok_or_else(|| "missing runtime error state".to_string())?;
        if !allow_overflow
            && !exchange_send_queue().reserve_bytes_for(
                addr.host(),
                addr.port() as u16,
                dest_finst_id,
                self.input.dest_node_id,
                self.sender_id,
                reserve_bytes,
            )
        {
            return Ok(PayloadEnqueue::NoCapacity(pending));
        }

        if let (Some(tracker), Some(accounting)) = (
            self.send_queue_mem_tracker.as_ref(),
            pending.accounting.as_mut(),
        ) {
            accounting.transfer_to(Arc::clone(tracker));
        }
        let task = self.build_exchange_send_task(
            addr.clone(),
            dest_finst_id,
            pending,
            Arc::clone(error_state),
        );
        if allow_overflow {
            exchange_send_queue().try_submit(task, true)?;
            return Ok(PayloadEnqueue::Enqueued);
        }
        exchange_send_queue().submit_reserved(task, reserve_bytes)?;
        Ok(PayloadEnqueue::Enqueued)
    }

    fn build_exchange_send_task(
        &self,
        destination: crate::runtime::endpoint::RuntimeEndpoint,
        dest_finst_id: UniqueId,
        pending: PendingPayload,
        error_state: Arc<RuntimeErrorState>,
    ) -> ExchangeSendTask {
        #[cfg(test)]
        record_payload_identity_for_test(self.fragment_instance_id, pending.be_number, pending.eos);
        ExchangeSendTask {
            frame: ExchangeFrame {
                destination,
                destination_fragment_instance_id: dest_finst_id,
                sender_fragment_instance_id: self.fragment_instance_id,
                destination_node_id: self.input.dest_node_id,
                sender_id: self.sender_id,
                backend_number: pending.be_number,
                eos: pending.eos,
                sequence: pending.sequence,
                payload: pending.payload,
            },
            transmitter: Arc::clone(&self.transmitter),
            payload_accounting: pending.accounting,
            encode_ns: pending.encode_ns,
            payload_bytes: pending.payload_bytes,
            profiles: self.profiles.clone(),
            notify: Arc::clone(&self.send_observable),
            error_state,
            tracker: Arc::clone(&self.send_tracker),
        }
    }

    fn should_include_wire_meta(chunks_empty: bool) -> bool {
        !chunks_empty
    }

    fn transmit_partition(
        &mut self,
        _dest_idx: usize,
        dest: &FragmentDestination,
        chunks: &[Chunk],
        eos: bool,
        allow_overflow: bool,
    ) -> Result<PayloadEnqueue, String> {
        use crate::novarocks_logging::debug;

        self.init_profile_if_needed();

        let dest_finst_id = UniqueId::new(dest.finst_id().high(), dest.finst_id().low());

        let row_count: usize = chunks.iter().map(|c| c.len()).sum();
        let sequence = self.shared_sequence.fetch_add(1, Ordering::SeqCst);
        debug!(
            "DataStreamSink::transmit_partition: dest_finst={} node_id={} sender_id={} chunks={} rows={} eos={} seq={}",
            dest_finst_id,
            self.input.dest_node_id,
            self.sender_id,
            chunks.len(),
            row_count,
            eos,
            sequence
        );

        let projected_storage;
        let chunks = if self.input.output_columns.is_empty() || chunks.is_empty() {
            chunks
        } else {
            projected_storage = chunks
                .iter()
                .map(|c| project_chunk_by_slot_ids(c, &self.input.output_columns))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| e.to_string())?;
            projected_storage.as_slice()
        };

        let be_number = self.be_number;
        // Delivery is asynchronous, so every data payload must be self-describing.
        let include_slot_ids = Self::should_include_wire_meta(chunks.is_empty());
        let encode_start = std::time::Instant::now();
        let payload = exchange::encode_chunks(chunks, include_slot_ids)
            .map_err(|e| format!("failed to encode chunks: {e}"))?;
        let encode_ns = encode_start.elapsed().as_nanos();
        let payload_bytes = payload.len();
        let payload_capacity_bytes = payload.capacity().max(payload_bytes);

        if let Some(profile) = self.profiles.as_ref() {
            profile.common.counter_add(
                "SerializeChunkTime",
                ProfileUnit::TimeNs,
                clamp_u128_to_i64(encode_ns),
            );
            profile.common.counter_add(
                "SerializedBytes",
                ProfileUnit::Bytes,
                clamp_u128_to_i64(payload_bytes as u128),
            );
        }

        let accounting = self
            .pending_payload_mem_tracker
            .as_ref()
            .map(|tracker| TrackedBytes::new(payload_capacity_bytes, Arc::clone(tracker)));
        let payload = PendingPayload {
            be_number,
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
                    dest_finst_id, self.input.dest_node_id, eos, sequence, payload_bytes
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
        let dests = self.destinations().to_vec();
        let per_dest_chunks = self.partition_chunk(&chunk)?;
        for (i, chunks) in per_dest_chunks.into_iter().enumerate() {
            if chunks.is_empty() || Self::is_pseudo_destination(&dests[i]) {
                continue;
            }
            // Upstream operators (notably hash join with a high per-key
            // fan-out) can emit a single chunk many MB in size — large enough
            // that its Arrow-IPC encoding exceeds `GRPC_MAX_MESSAGE_BYTES`
            // (64 MiB), causing h2 to reject the payload with a generic
            // protocol error. Slice oversize chunks down well below the wire
            // limit, but keep the slices large enough that downstream
            // per-chunk overhead (sort/analytic frame buffers) does not blow
            // up from a 360× fan-out. The drainer batches smaller chunks
            // back together up to `max_transmit_batched_bytes` per gRPC
            // payload, so the slice size is a hard upper bound only.
            let max_chunk_bytes = OVERSIZED_CHUNK_SPLIT_TARGET_BYTES;
            let mut chunks = split_oversized_chunks(chunks, max_chunk_bytes);
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
        let mut first_schema: Option<crate::exec::chunk::ChunkSchemaRef> = None;
        while let Some(front) = pending.front() {
            let chunk_bytes = front.batch.get_array_memory_size();
            if let Some(schema) = first_schema.as_ref()
                && front.chunk_schema() != schema.as_ref()
            {
                // Keep a single exchange payload schema-stable. Complex aggregate
                // outputs can diverge in nested nullability (for example MAP keys)
                // across drivers, and Arrow IPC normalization cannot widen those
                // nested field contracts without rebuilding the arrays.
                break;
            }
            if !batch.is_empty() && batch_bytes.saturating_add(chunk_bytes) > max_bytes {
                break;
            }
            let chunk = pending
                .pop_front()
                .ok_or_else(|| "pending buffer empty unexpectedly".to_string())?;
            batch_bytes = batch_bytes.saturating_add(chunk_bytes);
            if first_schema.is_none() {
                first_schema = Some(chunk.chunk_schema_ref());
            }
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
        let dests: Vec<FragmentDestination> = self.destinations().to_vec();
        for (i, dest) in dests.iter().enumerate() {
            if Self::is_pseudo_destination(dest) {
                continue;
            }
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
                if !self.should_flush_pending_dest(i, force) {
                    break;
                }
                let (chunks, _batch_bytes) = self.drain_pending_batch(i)?;
                if chunks.is_empty() {
                    break;
                }
                match self.transmit_partition(i, dest, &chunks, false, allow_overflow)? {
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
        let dests: Vec<FragmentDestination> = self.destinations().to_vec();
        for (i, dest) in dests.iter().enumerate() {
            // No fragment instance is running for pseudo destinations — do not send EOS.
            if Self::is_pseudo_destination(dest) {
                continue;
            }
            match self.transmit_partition(i, dest, &[], true, true)? {
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
    fn accepts_encoded_column(&self, _slot_id: SlotId, data_type: &DataType) -> bool {
        is_low_cardinality_exchange_dictionary(data_type)
            && matches!(
                self.input.output_partition_type,
                DataStreamPartitionType::Unpartitioned
                    | DataStreamPartitionType::Random
                    | DataStreamPartitionType::HashPartitioned
                    | DataStreamPartitionType::BucketShuffleHashPartitioned
            )
    }

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
        self.sync_be_number(_state);
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
        self.sync_be_number(_state);
        self.ensure_error_state(_state);
        if let Some(err) = self.current_error() {
            return Err(err);
        }
        self.init_profile_if_needed();
        self.finishing.store(true, Ordering::Release);

        debug!(
            "DataStreamSink set_finishing: finst={} driver_id={} dest_node_id={} sender_id={} be_number={} destinations={} pending_dests={} pending_chunk_bytes_total={} pending_payload_bytes_total={}",
            format_uuid(
                self.fragment_instance_id.high(),
                self.fragment_instance_id.low()
            ),
            self.driver_id,
            self.input.dest_node_id,
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
                self.fragment_instance_id.high(),
                self.fragment_instance_id.low()
            ),
            self.driver_id,
            self.input.dest_node_id,
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

/// Target slice size for [`split_oversized_chunks`]. Sits well below
/// `GRPC_MAX_MESSAGE_BYTES` (64 MiB) so even with Arrow-IPC framing overhead
/// the encoded payload stays under the wire limit, while large enough that a
/// runaway hash-join chunk produces only a handful of slices — not the
/// hundreds we'd get if we reused the much smaller `max_transmit_batched_bytes`
/// (256 KiB) value, which blew up downstream window-operator per-chunk
/// bookkeeping for the F1-with_join JOIN-then-window pipeline.
const OVERSIZED_CHUNK_SPLIT_TARGET_BYTES: usize = 16 * 1024 * 1024;

/// Slice each chunk that exceeds `target_bytes` in memory into a series of
/// smaller chunks whose individual in-memory size sits at or below the
/// target. The split row count is derived from the chunk's average per-row
/// memory footprint, so wide rows yield smaller slices and narrow rows yield
/// larger ones. Chunks already under the target are returned untouched.
fn split_oversized_chunks(chunks: Vec<Chunk>, target_bytes: usize) -> Vec<Chunk> {
    let target_bytes = target_bytes.max(1);
    let mut out = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        let bytes = chunk.batch.get_array_memory_size();
        let rows = chunk.batch.num_rows();
        if rows <= 1 || bytes <= target_bytes {
            out.push(chunk);
            continue;
        }
        // ceil_div(bytes, target_bytes) — number of slices needed for the
        // chunk to fit under the target. `bytes_per_row` is the implied
        // per-row footprint; rows_per_slice is sized so that one slice is
        // ~target_bytes.
        let bytes_per_row = bytes.div_ceil(rows).max(1);
        let rows_per_slice = (target_bytes / bytes_per_row).max(1);
        let mut offset = 0;
        while offset < rows {
            let len = rows_per_slice.min(rows - offset);
            out.push(chunk.slice(offset, len));
            offset += len;
        }
    }
    out
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
    let projected_slots = slot_ids
        .iter()
        .map(|slot_id| {
            chunk.chunk_schema().slot(*slot_id).cloned().ok_or_else(|| {
                format!(
                    "output_columns slot id {} missing from chunk schema",
                    slot_id
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let _ = new_schema;
    Chunk::try_new_with_columns(
        Arc::new(crate::exec::chunk::ChunkSchema::try_new(projected_slots)?),
        cols,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
    use arrow::array::{
        Array, ArrayRef, BinaryArray, DictionaryArray, Int32Array, LargeStringArray, StringArray,
    };
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use arrow::record_batch::RecordBatchOptions;

    use crate::runtime::endpoint::RuntimeEndpoint;

    fn make_test_operator() -> DataStreamSinkOperator {
        DataStreamSinkOperator {
            name: "DataStreamSink(test)".to_string(),
            input: DataStreamSinkFactoryInput::try_new(
                1,
                DataStreamPartitionType::Unpartitioned,
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
            )
            .expect("test sink input"),
            transmitter: crate::runtime::fragment::io::exchange::discard_exchange_transmitter(),
            fragment_instance_id: UniqueId::new(1, 2),
            arena: ExprArena::default(),
            expr_ids: Vec::new(),
            init_error: None,
            driver_id: 0,
            sender_id: 11,
            be_number: 0,
            shared_sequence: Arc::new(AtomicI64::new(0)),
            random_next: 0,
            pending_per_dest: Vec::new(),
            pending_bytes_per_dest: Vec::new(),
            pending_payloads_per_dest: Vec::new(),
            max_transmit_batched_bytes: 1,
            finished: AtomicBool::new(false),
            finishing: AtomicBool::new(false),
            send_tracker: ExchangeSendTracker::new(),
            send_observable: Arc::new(Observable::new()),
            error_state: None,
            finish_state: Arc::new(DataStreamSinkFinishState::default()),
            profile_initialized: false,
            profiles: None,
            pending_chunks_mem_tracker: None,
            pending_payload_mem_tracker: None,
            send_queue_mem_tracker: None,
        }
    }

    fn make_test_destination() -> FragmentDestination {
        FragmentDestination::new(
            UniqueId::new(9, 9),
            RuntimeEndpoint::new("127.0.0.1", 9030).expect("endpoint"),
        )
    }

    fn make_test_exchange_send_task() -> ExchangeSendTask {
        let mut op = make_test_operator();
        op.fragment_instance_id = UniqueId::new(81, 82);
        op.build_exchange_send_task(
            RuntimeEndpoint::new("127.0.0.1", 9030).expect("endpoint"),
            UniqueId::new(91, 92),
            PendingPayload {
                be_number: 7,
                payload: vec![1, 2, 3],
                payload_bytes: 3,
                encode_ns: 123,
                sequence: 99,
                eos: false,
                accounting: None,
            },
            Arc::new(RuntimeErrorState::default()),
        )
    }

    fn zero_column_chunk_with_rows(row_count: usize) -> Chunk {
        let options = RecordBatchOptions::new().with_row_count(Some(row_count));
        let batch = arrow::array::RecordBatch::try_new_with_options(
            Arc::new(arrow::datatypes::Schema::empty()),
            vec![],
            &options,
        )
        .expect("zero-column batch");
        Chunk::new_with_chunk_schema(batch, Arc::new(crate::exec::chunk::ChunkSchema::empty()))
    }

    #[test]
    fn data_stream_sink_accepts_low_cardinality_dictionary_carriers() {
        let op = make_test_operator();
        let utf8_dict = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let large_utf8_dict =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::LargeUtf8));

        assert!(op.accepts_encoded_column(SlotId::new(91), &utf8_dict));
        assert!(op.accepts_encoded_column(SlotId::new(91), &large_utf8_dict));
    }

    #[test]
    fn data_stream_sink_rejects_non_string_or_non_int32_dictionaries() {
        let op = make_test_operator();
        let wrong_key = DataType::Dictionary(Box::new(DataType::Int64), Box::new(DataType::Utf8));
        let wrong_value =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int32));

        assert!(!op.accepts_encoded_column(SlotId::new(91), &wrong_key));
        assert!(!op.accepts_encoded_column(SlotId::new(91), &wrong_value));
        assert!(!op.accepts_encoded_column(SlotId::new(91), &DataType::Utf8));
    }

    #[test]
    fn data_stream_sink_accepts_dictionary_for_hash_partition_after_hash_support() {
        let mut op = make_test_operator();
        op.input.output_partition_type = DataStreamPartitionType::HashPartitioned;
        let utf8_dict = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));

        assert!(op.accepts_encoded_column(SlotId::new(91), &utf8_dict));
    }

    #[test]
    fn bind_runtime_state_uses_backend_num_as_be_number() {
        let mut op = make_test_operator();
        let state = RuntimeState::new(None, None, None, None, Some(7), None, None, None);

        Operator::bind_runtime_state(&mut op, &state).expect("bind runtime state");

        assert_eq!(op.sender_id, 11);
        assert_eq!(op.be_number, 7);
    }

    #[test]
    fn zero_byte_pending_chunks_are_tracked_for_force_flush() {
        let mut op = make_test_operator();
        op.input.destinations = vec![make_test_destination()];

        op.buffer_chunk(zero_column_chunk_with_rows(1))
            .expect("buffer chunk");

        assert_eq!(op.pending_chunk_bytes_total(), 0);
        assert_eq!(op.pending_chunk_count_total(), 1);
        assert!(op.has_pending_chunks());
        assert!(op.has_pending_data());
        assert!(!op.should_flush_pending_dest(0, false));
        assert!(op.should_flush_pending_dest(0, true));
    }

    #[test]
    fn queued_payload_preserves_encoded_backend_number() {
        let mut op = make_test_operator();
        op.be_number = 8;
        op.pending_payloads_per_dest = vec![Some(PendingPayload {
            be_number: 7,
            payload: vec![1],
            payload_bytes: 1,
            encode_ns: 0,
            sequence: 0,
            eos: false,
            accounting: None,
        })];

        let payload = op.pending_payloads_per_dest[0]
            .take()
            .expect("pending payload should exist");

        assert_eq!(
            payload.be_number, 7,
            "pending payload must keep its encoded backend number, not current operator state"
        );
    }

    #[test]
    fn exchange_send_task_carries_distinct_dest_and_sender_finsts() {
        let task = make_test_exchange_send_task();

        assert_eq!(
            task.frame.destination_fragment_instance_id,
            UniqueId::new(91, 92)
        );
        assert_eq!(
            task.frame.sender_fragment_instance_id,
            UniqueId::new(81, 82)
        );
    }

    #[test]
    fn wire_meta_is_included_for_every_non_empty_payload() {
        assert!(
            DataStreamSinkOperator::should_include_wire_meta(false),
            "non-empty payloads must carry wire meta because enqueue is not delivery confirmation"
        );
        assert!(!DataStreamSinkOperator::should_include_wire_meta(true));
    }

    #[test]
    fn hash_partition_accepts_binary_arrays() {
        let array = Arc::new(BinaryArray::from(vec![
            Some(b"a".as_slice()),
            Some(b"b".as_slice()),
            None,
        ]));
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Binary, true)]));
        let batch = arrow::array::RecordBatch::try_new(schema, vec![array.clone()]).unwrap();
        let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[crate::common::ids::SlotId::new(1)],
        )
        .unwrap();
        let chunk = Chunk::new_with_chunk_schema(batch, chunk_schema);

        let partitions =
            partition_chunk_by_hash_arrays(&chunk, &[array], 2, false).expect("partition");

        assert_eq!(
            partitions
                .iter()
                .map(|chunk| chunk.batch.num_rows())
                .sum::<usize>(),
            3
        );
    }

    fn status_chunk_from_array(array: ArrayRef) -> Chunk {
        let schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(91),
                Field::new("status", DataType::Utf8, true),
                None,
                None,
            )])
            .expect("chunk schema"),
        );
        Chunk::try_new_with_columns(schema, vec![array]).expect("status chunk")
    }

    fn large_status_chunk_from_array(array: ArrayRef) -> Chunk {
        let schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(92),
                Field::new("status_l", DataType::LargeUtf8, true),
                None,
                None,
            )])
            .expect("chunk schema"),
        );
        Chunk::try_new_with_columns(schema, vec![array]).expect("large status chunk")
    }

    fn status_values(chunk: &Chunk) -> Vec<Option<String>> {
        if chunk.is_empty() {
            return Vec::new();
        }
        let flat = arrow::compute::cast(chunk.columns()[0].as_ref(), &DataType::Utf8)
            .expect("cast status to utf8");
        let strings = flat
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 values");
        (0..strings.len())
            .map(|idx| {
                if strings.is_null(idx) {
                    None
                } else {
                    Some(strings.value(idx).to_string())
                }
            })
            .collect()
    }

    fn partition_status_values(chunks: &[Chunk]) -> Vec<Vec<Option<String>>> {
        chunks.iter().map(status_values).collect()
    }

    fn flat_and_dict_status_chunks() -> (Chunk, ArrayRef, Chunk, ArrayRef) {
        let flat: ArrayRef = Arc::new(StringArray::from(vec![
            Some("PAID"),
            Some("NEW"),
            None,
            Some("PAID"),
            Some("CANCELLED"),
            Some("NEW"),
        ]));
        let dict: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::new(
            Int32Array::from(vec![Some(0), Some(1), None, Some(0), Some(2), Some(1)]),
            Arc::new(StringArray::from(vec!["PAID", "NEW", "CANCELLED"])),
        ));
        (
            status_chunk_from_array(flat.clone()),
            flat,
            status_chunk_from_array(dict.clone()),
            dict,
        )
    }

    fn flat_and_dict_large_status_chunks() -> (Chunk, ArrayRef, Chunk, ArrayRef) {
        let flat: ArrayRef = Arc::new(LargeStringArray::from(vec![
            Some("PAID"),
            Some("NEW"),
            None,
            Some("PAID"),
            Some("CANCELLED"),
            Some("NEW"),
        ]));
        let dict: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::new(
            Int32Array::from(vec![Some(2), Some(0), None, Some(2), Some(1), Some(0)]),
            Arc::new(LargeStringArray::from(vec!["NEW", "CANCELLED", "PAID"])),
        ));
        (
            large_status_chunk_from_array(flat.clone()),
            flat,
            large_status_chunk_from_array(dict.clone()),
            dict,
        )
    }

    #[test]
    fn hash_partition_dictionary_utf8_matches_flat_utf8_fnv() {
        let (flat_chunk, flat_array, dict_chunk, dict_array) = flat_and_dict_status_chunks();

        let flat_parts =
            partition_chunk_by_hash_arrays(&flat_chunk, &[flat_array], 4, false).expect("flat fnv");
        let dict_parts =
            partition_chunk_by_hash_arrays(&dict_chunk, &[dict_array], 4, false).expect("dict fnv");

        assert_eq!(
            partition_status_values(&dict_parts),
            partition_status_values(&flat_parts)
        );
    }

    #[test]
    fn hash_partition_dictionary_utf8_matches_flat_utf8_crc32() {
        let (flat_chunk, flat_array, dict_chunk, dict_array) = flat_and_dict_status_chunks();

        let flat_parts =
            partition_chunk_by_hash_arrays(&flat_chunk, &[flat_array], 4, true).expect("flat crc");
        let dict_parts =
            partition_chunk_by_hash_arrays(&dict_chunk, &[dict_array], 4, true).expect("dict crc");

        assert_eq!(
            partition_status_values(&dict_parts),
            partition_status_values(&flat_parts)
        );
    }

    #[test]
    fn hash_partition_dictionary_large_utf8_matches_flat_large_utf8_fnv() {
        let (flat_chunk, flat_array, dict_chunk, dict_array) = flat_and_dict_large_status_chunks();

        let flat_parts =
            partition_chunk_by_hash_arrays(&flat_chunk, &[flat_array], 4, false).expect("flat fnv");
        let dict_parts =
            partition_chunk_by_hash_arrays(&dict_chunk, &[dict_array], 4, false).expect("dict fnv");

        assert_eq!(
            partition_status_values(&dict_parts),
            partition_status_values(&flat_parts)
        );
    }

    #[test]
    fn hash_partition_dictionary_large_utf8_matches_flat_large_utf8_crc32() {
        let (flat_chunk, flat_array, dict_chunk, dict_array) = flat_and_dict_large_status_chunks();

        let flat_parts =
            partition_chunk_by_hash_arrays(&flat_chunk, &[flat_array], 4, true).expect("flat crc");
        let dict_parts =
            partition_chunk_by_hash_arrays(&dict_chunk, &[dict_array], 4, true).expect("dict crc");

        assert_eq!(
            partition_status_values(&dict_parts),
            partition_status_values(&flat_parts)
        );
    }

    fn int64_chunk(rows: usize) -> Chunk {
        let array = Arc::new(arrow::array::Int64Array::from_iter_values(0..rows as i64))
            as arrow::array::ArrayRef;
        let field = Arc::new(Field::new("v", DataType::Int64, false));
        let schema = Arc::new(Schema::new(vec![field.as_ref().clone()]));
        let batch = arrow::array::RecordBatch::try_new(schema, vec![array]).expect("batch");
        let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId(1)],
        )
        .expect("chunk schema");
        Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk")
    }

    #[test]
    fn split_oversized_chunks_passes_small_chunks_through_unchanged() {
        let chunks = vec![int64_chunk(8)];
        let out = split_oversized_chunks(chunks, 1024 * 1024);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].batch.num_rows(), 8);
    }

    #[test]
    fn split_oversized_chunks_slices_large_chunk_into_pieces_with_row_count_preserved() {
        // 10_000 int64 rows ≈ 80 KiB in-memory. Slice at a 8 KiB target. The
        // sliced chunks share the underlying Arrow buffers and therefore
        // still report the same `get_array_memory_size`, but the encoded IPC
        // payload (which is the wire size we actually care about) is sized
        // to the logical row range, so the slice count and row preservation
        // are what matters here.
        let rows = 10_000;
        let target_bytes = 8 * 1024;
        let chunks = vec![int64_chunk(rows)];
        let out = split_oversized_chunks(chunks, target_bytes);
        assert!(out.len() > 1, "expected multiple slices, got {}", out.len());
        let total_rows: usize = out.iter().map(|c| c.batch.num_rows()).sum();
        assert_eq!(total_rows, rows);
        for piece in &out {
            assert!(piece.batch.num_rows() > 0, "slice produced zero-row chunk");
        }
    }

    #[test]
    fn split_oversized_chunks_handles_single_row_oversized_chunk() {
        // A chunk with a single row that nonetheless exceeds the target
        // (e.g. a very wide row) should be returned untouched rather than
        // split into zero-row slices.
        let chunks = vec![int64_chunk(1)];
        let out = split_oversized_chunks(chunks, 1);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].batch.num_rows(), 1);
    }
}
