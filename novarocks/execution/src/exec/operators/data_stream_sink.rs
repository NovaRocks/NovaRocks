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

#[cfg(test)]
use crate::exec::pipeline::operator::ExitProgress;
use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::runtime::task::{ExecutionFailure, ExecutionResult};

#[derive(Clone, Debug, Eq, PartialEq)]
struct StreamStepContract(novarocks_failure_contract::BoundedDetail);
impl std::fmt::Display for StreamStepContract {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for StreamStepContract {}
impl novarocks_failure_contract::FailureProjection for StreamStepContract {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Invariant,
            crate::runtime::task::ExecutionReason::ExchangeStreamContract.reason(),
        )
    }
}
fn stream_contract(detail: impl std::fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from_domain(StreamStepContract(
        novarocks_failure_contract::BoundedDetail::truncating_display(detail),
    ))
}

#[derive(Debug)]
struct StreamPartitionArrowFailure(arrow::error::ArrowError);
impl std::fmt::Display for StreamPartitionArrowFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for StreamPartitionArrowFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl novarocks_failure_contract::FailureProjection for StreamPartitionArrowFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::task::ExecutionReason;
        use arrow::error::ArrowError;
        use novarocks_failure_contract::{
            FailureClass, FailureFact, RepresentationBoundary, RepresentationFacts,
            RepresentationLayout, domain,
        };
        let (class, reason) = match &self.0 {
            ArrowError::MemoryError(_) | ArrowError::ArithmeticOverflow(_) => (
                FailureClass::Resource,
                ExecutionReason::ExchangePartitionAllocation,
            ),
            ArrowError::OffsetOverflowError(_) => (
                FailureClass::Representation,
                ExecutionReason::ExchangePartitionOffset,
            ),
            ArrowError::IoError(..) | ArrowError::ExternalError(_) => (
                FailureClass::Unexpected,
                ExecutionReason::ExchangePartitionUnexpected,
            ),
            _ => (
                FailureClass::Invariant,
                ExecutionReason::ExchangePartitionArrow,
            ),
        };
        let fact = FailureFact::new(class, reason.reason());
        if matches!(self.0, ArrowError::OffsetOverflowError(_)) {
            // The selected Arrow take/cast boundary knows neither the complete
            // required extent nor a safe independent output prefix.
            fact.with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::SingleOutput,
                layout: RepresentationLayout {
                    domain: domain::EXECUTION,
                    code: std::num::NonZeroU16::new(15).unwrap(),
                },
                required: None,
                extent_limit: None,
                representable_prefix: None,
            })
        } else {
            fact
        }
    }
}
fn partition_arrow(source: arrow::error::ArrowError) -> ExecutionFailure {
    ExecutionFailure::from_domain(StreamPartitionArrowFailure(source))
}
#[derive(Debug)]
struct StreamDictionaryKeyFailure {
    row: usize,
    key: i32,
    source: std::num::TryFromIntError,
}
impl std::fmt::Display for StreamDictionaryKeyFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "hash_partition: negative dictionary key {} at row {}",
            self.key, self.row
        )
    }
}
impl std::error::Error for StreamDictionaryKeyFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl novarocks_failure_contract::FailureProjection for StreamDictionaryKeyFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Invariant,
            crate::runtime::task::ExecutionReason::ExchangeDictionaryKey.reason(),
        )
    }
}
#[derive(Clone, Copy, Debug)]
struct PartitionRowExtent {
    last_index: u64,
}
impl std::fmt::Display for PartitionRowExtent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "exchange partition row index {} exceeds UInt32",
            self.last_index
        )
    }
}
impl std::error::Error for PartitionRowExtent {}
impl novarocks_failure_contract::FailureProjection for PartitionRowExtent {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{
            FailureClass, FailureFact, RepresentationBoundary, RepresentationFacts,
            RepresentationLayout, domain,
        };
        FailureFact::new(
            FailureClass::Representation,
            crate::runtime::task::ExecutionReason::ExchangePartitionRowExtent.reason(),
        )
        .with_representation(RepresentationFacts {
            boundary: RepresentationBoundary::SingleOutput,
            layout: RepresentationLayout {
                domain: domain::EXECUTION,
                code: std::num::NonZeroU16::new(16).unwrap(),
            },
            required: Some(self.last_index),
            extent_limit: Some(u64::from(u32::MAX)),
            representable_prefix: None,
        })
    }
}
fn check_partition_row_extent(rows: usize) -> ExecutionResult<()> {
    let last_index = rows.saturating_sub(1);
    if last_index as u128 > u128::from(u32::MAX) {
        return Err(ExecutionFailure::from_domain(PartitionRowExtent {
            last_index: last_index as u64,
        }));
    }
    Ok(())
}

use crate::exec::chunk::Chunk;
use crate::exec::expr::compiled_program::CompiledExpressionInstance;
use crate::exec::fragment::sink::{DataStreamPartitionType, DataStreamSinkFactoryInput};
use crate::exec::operators::compiled_expression::{
    RuntimeKernelControl, evaluate_all, evaluate_selected, instances as compiled_instances,
    root_value_type,
};
use crate::runtime::endpoint::TaskDestination;
use crate::runtime::mem_tracker::{MemTracker, TrackedBytes};
use crate::runtime::task::io::exchange_retained_payload::ExchangeRetainedPayload;
use crate::runtime::task::io::exchange_sender_group::{ExchangeSenderGroup, GroupPublication};
use crate::runtime::task::io::exchange_stream_sender::ExchangeStreamSenderConfig;
use crate::runtime::task::io::{
    EdgeSendGate, EdgeSendState, ExchangeDestinationKey, ExchangeEdgeGates,
    ExchangeFrameTransmitter,
};
use crate::task_execution::domain::ExchangeEdgeId;
use arrow::array::{Array, ArrayRef, BooleanArray};
use arrow::datatypes::DataType;
use novarocks_local_program::{LocalProgram, ProgramExpressionRootSite};
use novarocks_types::SlotId;
use novarocks_types::{UniqueId, format_uuid};
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
#[cfg(test)]
use std::sync::{Mutex, OnceLock};

use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::exec::pipeline::{
    driver_step::DriverStep,
    input::{PreparedInput, RawInput},
    operator::{ExitObservation, FinishWatch, InputEncodingPolicy, Operator},
    shape::{ControlStep, SinkKernel, SinkStep, Transform},
    step::{AdmissionNext, NextStep, WaitFact, WaitReason},
};
use crate::runtime::profile::{ProfileUnit, clamp_u128_to_i64};
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};

const NEED_INPUT_LOG_EVERY: u64 = 1;

static NEED_INPUT_BLOCKED_LOG_COUNT: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
static DATA_STREAM_PAYLOAD_IDENTITIES: OnceLock<Mutex<Vec<(UniqueId, i32, bool)>>> =
    OnceLock::new();

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Retained for distributed sink end-of-stream assertions."
)]
pub(crate) fn take_eos_be_number_for_test(task_key: UniqueId) -> Option<i32> {
    let mut identities = DATA_STREAM_PAYLOAD_IDENTITIES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .expect("data stream payload identity lock");
    let position = identities
        .iter()
        .position(|(recorded_task_key, _, eos)| *recorded_task_key == task_key && *eos)?;
    Some(identities.swap_remove(position).1)
}

#[cfg(test)]
fn record_payload_identity_for_test(task_key: UniqueId, be_number: i32, eos: bool) {
    DATA_STREAM_PAYLOAD_IDENTITIES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .expect("data stream payload identity lock")
        .push((task_key, be_number, eos));
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
    use super::{
        Chunk, StreamDictionaryKeyFailure, check_partition_row_extent,
        is_low_cardinality_exchange_dictionary, partition_arrow, stream_contract,
    };
    use crate::exec::hash_table::hash::{canonical_f32_bits, canonical_f64_bits};
    use crate::runtime::task::{ExecutionFailure, ExecutionResult};
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

    use crate::exec::hash_table::key_builder::{
        canonical_group_key_crc32_hash, canonical_group_key_fnv_hash,
    };
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

    fn crc32_update(mut crc: u32, value: &[u8]) -> u32 {
        for &byte in value {
            crc ^= byte as u32;
            for _ in 0..8 {
                crc = if crc & 1 != 0 {
                    (crc >> 1) ^ 0xedb88320
                } else {
                    crc >> 1
                };
            }
        }
        crc
    }

    fn crc32_hash_value(value: &[u8]) -> u32 {
        crc32_update(0xffff_ffff, value) ^ 0xffff_ffff
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

    fn dictionary_int32_indices(array: &ArrayRef) -> ExecutionResult<&DictionaryArray<Int32Type>> {
        array
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .ok_or_else(|| {
                stream_contract(format_args!(
                    "hash_partition: failed to downcast dictionary carrier {:?} to DictionaryArray<Int32Type>",
                    array.data_type()
                ))
            })
    }

    fn dictionary_key_index(key: i32, values_len: usize, row: usize) -> ExecutionResult<usize> {
        let idx = usize::try_from(key).map_err(|source| {
            ExecutionFailure::from_domain(StreamDictionaryKeyFailure { row, key, source })
        })?;
        if idx >= values_len {
            return Err(stream_contract(format_args!(
                "hash_partition: dictionary key {key} at row {row} exceeds values len {values_len}"
            )));
        }
        Ok(idx)
    }

    fn compute_fnv_hash_dictionary_array(array: &ArrayRef) -> ExecutionResult<Vec<u64>> {
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

    fn compute_crc32_hash_dictionary_array(array: &ArrayRef) -> ExecutionResult<Vec<u32>> {
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
    fn compute_fnv_hash_array(array: &ArrayRef) -> ExecutionResult<Vec<u64>> {
        let len = array.len();
        let mut hash_values = vec![FNV_SEED; len];

        match array.data_type() {
            DataType::Boolean => {
                let arr = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| stream_contract("failed to downcast to BooleanArray"))?;
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
                    .ok_or_else(|| stream_contract("failed to downcast to Int64Array"))?;
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
                    .ok_or_else(|| stream_contract("failed to downcast to Int32Array"))?;
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
                    .ok_or_else(|| stream_contract("failed to downcast to Int16Array"))?;
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
                    .ok_or_else(|| stream_contract("failed to downcast to Float64Array"))?;
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if arr.is_null(i) {
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    } else {
                        let val = arr.value(i);
                        let bytes = canonical_f64_bits(val).to_le_bytes();
                        *hash_value ^= fnv_hash_value(&bytes);
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    }
                }
            }
            DataType::Float32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| stream_contract("failed to downcast to Float32Array"))?;
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if arr.is_null(i) {
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    } else {
                        let val = arr.value(i);
                        let bytes = canonical_f32_bits(val).to_le_bytes();
                        *hash_value ^= fnv_hash_value(&bytes);
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    }
                }
            }
            DataType::Utf8 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| stream_contract("failed to downcast to StringArray"))?;
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
                    .ok_or_else(|| stream_contract("failed to downcast to LargeStringArray"))?;
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
                    .ok_or_else(|| stream_contract("failed to downcast to BinaryArray"))?;
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
                    .ok_or_else(|| stream_contract("failed to downcast to LargeBinaryArray"))?;
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
                    .ok_or_else(|| stream_contract("failed to downcast to Date32Array"))?;
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
                                        stream_contract(
                                            "failed to downcast to TimestampSecondArray",
                                        )
                                    })?;
                                arr.value(i)
                            }
                            TimeUnit::Millisecond => {
                                let arr = array
                                    .as_any()
                                    .downcast_ref::<TimestampMillisecondArray>()
                                    .ok_or_else(|| {
                                        stream_contract(
                                            "failed to downcast to TimestampMillisecondArray",
                                        )
                                    })?;
                                arr.value(i)
                            }
                            TimeUnit::Microsecond => {
                                let arr = array
                                    .as_any()
                                    .downcast_ref::<TimestampMicrosecondArray>()
                                    .ok_or_else(|| {
                                        stream_contract(
                                            "failed to downcast to TimestampMicrosecondArray",
                                        )
                                    })?;
                                arr.value(i)
                            }
                            TimeUnit::Nanosecond => {
                                let arr = array
                                    .as_any()
                                    .downcast_ref::<TimestampNanosecondArray>()
                                    .ok_or_else(|| {
                                        stream_contract(
                                            "failed to downcast to TimestampNanosecondArray",
                                        )
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
                    .ok_or_else(|| stream_contract("failed to downcast to Decimal128Array"))?;
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
                    .ok_or_else(|| stream_contract("failed to downcast to Decimal256Array"))?;
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
                    .ok_or_else(|| stream_contract("failed to downcast to FixedSizeBinaryArray"))?;
                if arr.value_length() != 16 {
                    return Err(stream_contract(
                        "hash_partition LARGEINT physical width differs from 16",
                    ));
                }
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if arr.is_null(i) {
                        *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                    } else {
                        // FixedSizeBinaryArray owns this exact fixed-width slice.
                        let fixed: [u8; 16] = arr
                            .value(i)
                            .try_into()
                            .expect("checked LARGEINT physical width");
                        let value = i128::from_be_bytes(fixed);
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
                    .ok_or_else(|| stream_contract("failed to downcast to ListArray"))?;
                let values = list
                    .values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        stream_contract("failed to downcast list values to StringArray")
                    })?;
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
                    .ok_or_else(|| stream_contract("failed to downcast to ListArray"))?;
                let values = list
                    .values()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| {
                        stream_contract("failed to downcast list values to Int32Array")
                    })?;
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
                    match canonical_group_key_fnv_hash(array, row)? {
                        Some(row_hash) => {
                            *hash_value ^= row_hash;
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
                return Err(stream_contract(format_args!(
                    "hash_partition: unsupported array type for FNV hash: {:?}",
                    array.data_type()
                )));
            }
        }

        Ok(hash_values)
    }

    // Compute CRC32 hash for each row in an array (for BUCKET_SHUFFLE_HASH_PARTITIONED)
    fn compute_crc32_hash_array(array: &ArrayRef) -> ExecutionResult<Vec<u32>> {
        let len = array.len();
        let mut hash_values = vec![0u32; len];

        match array.data_type() {
            DataType::Boolean => {
                let arr = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| stream_contract("failed to downcast to BooleanArray"))?;
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
                    .ok_or_else(|| stream_contract("failed to downcast to Int64Array"))?;
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
                    .ok_or_else(|| stream_contract("failed to downcast to Int32Array"))?;
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
                    .ok_or_else(|| stream_contract("failed to downcast to Int16Array"))?;
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
                    .ok_or_else(|| stream_contract("failed to downcast to Float64Array"))?;
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if !arr.is_null(i) {
                        let val = arr.value(i);
                        let bytes = canonical_f64_bits(val).to_le_bytes();
                        *hash_value = crc32_hash_value(&bytes);
                    }
                }
            }
            DataType::Float32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| stream_contract("failed to downcast to Float32Array"))?;
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if !arr.is_null(i) {
                        let val = arr.value(i);
                        let bytes = canonical_f32_bits(val).to_le_bytes();
                        *hash_value = crc32_hash_value(&bytes);
                    }
                }
            }
            DataType::Utf8 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| stream_contract("failed to downcast to StringArray"))?;
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
                    .ok_or_else(|| stream_contract("failed to downcast to LargeStringArray"))?;
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
                    .ok_or_else(|| stream_contract("failed to downcast to BinaryArray"))?;
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
                    .ok_or_else(|| stream_contract("failed to downcast to LargeBinaryArray"))?;
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
                    .ok_or_else(|| stream_contract("failed to downcast to Date32Array"))?;
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
                                        stream_contract(
                                            "failed to downcast to TimestampSecondArray",
                                        )
                                    })?;
                                arr.value(i)
                            }
                            TimeUnit::Millisecond => {
                                let arr = array
                                    .as_any()
                                    .downcast_ref::<TimestampMillisecondArray>()
                                    .ok_or_else(|| {
                                        stream_contract(
                                            "failed to downcast to TimestampMillisecondArray",
                                        )
                                    })?;
                                arr.value(i)
                            }
                            TimeUnit::Microsecond => {
                                let arr = array
                                    .as_any()
                                    .downcast_ref::<TimestampMicrosecondArray>()
                                    .ok_or_else(|| {
                                        stream_contract(
                                            "failed to downcast to TimestampMicrosecondArray",
                                        )
                                    })?;
                                arr.value(i)
                            }
                            TimeUnit::Nanosecond => {
                                let arr = array
                                    .as_any()
                                    .downcast_ref::<TimestampNanosecondArray>()
                                    .ok_or_else(|| {
                                        stream_contract(
                                            "failed to downcast to TimestampNanosecondArray",
                                        )
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
                    .ok_or_else(|| stream_contract("failed to downcast to Decimal128Array"))?;
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
                    .ok_or_else(|| stream_contract("failed to downcast to Decimal256Array"))?;
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
                    .ok_or_else(|| stream_contract("failed to downcast to FixedSizeBinaryArray"))?;
                if arr.value_length() != 16 {
                    return Err(stream_contract(
                        "hash_partition LARGEINT physical width differs from 16",
                    ));
                }
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if !arr.is_null(i) {
                        // FixedSizeBinaryArray owns this exact fixed-width slice.
                        let fixed: [u8; 16] = arr
                            .value(i)
                            .try_into()
                            .expect("checked LARGEINT physical width");
                        let value = i128::from_be_bytes(fixed);
                        let bytes = value.to_le_bytes();
                        *hash_value = crc32_hash_value(&bytes);
                    }
                }
            }
            DataType::List(field) if matches!(field.data_type(), DataType::Utf8) => {
                let list = array
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| stream_contract("failed to downcast to ListArray"))?;
                let values = list
                    .values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        stream_contract("failed to downcast list values to StringArray")
                    })?;
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if list.is_null(i) {
                        continue;
                    }
                    let offsets = list.value_offsets();
                    let start = offsets[i] as usize;
                    let end = offsets[i + 1] as usize;
                    let mut crc = crc32_update(
                        0xffff_ffff,
                        &(end.saturating_sub(start) as u64).to_le_bytes(),
                    );
                    for idx in start..end {
                        if values.is_null(idx) {
                            crc = crc32_update(crc, &[0]);
                        } else {
                            crc = crc32_update(crc, &[1]);
                            crc = crc32_update(crc, values.value(idx).as_bytes());
                        }
                    }
                    *hash_value = crc ^ 0xffff_ffff;
                }
            }
            DataType::List(field) if matches!(field.data_type(), DataType::Int32) => {
                let list = array
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| stream_contract("failed to downcast to ListArray"))?;
                let values = list
                    .values()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| {
                        stream_contract("failed to downcast list values to Int32Array")
                    })?;
                for (i, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if list.is_null(i) {
                        continue;
                    }
                    let offsets = list.value_offsets();
                    let start = offsets[i] as usize;
                    let end = offsets[i + 1] as usize;
                    let mut crc = crc32_update(
                        0xffff_ffff,
                        &(end.saturating_sub(start) as u64).to_le_bytes(),
                    );
                    for idx in start..end {
                        if values.is_null(idx) {
                            crc = crc32_update(crc, &[0]);
                        } else {
                            crc = crc32_update(crc, &[1]);
                            crc = crc32_update(crc, &values.value(idx).to_le_bytes());
                        }
                    }
                    *hash_value = crc ^ 0xffff_ffff;
                }
            }
            DataType::List(_) | DataType::Struct(_) | DataType::Map(_, _) => {
                for (row, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if let Some(row_hash) = canonical_group_key_crc32_hash(array, row)? {
                        *hash_value = row_hash;
                    }
                }
            }
            dict_type if is_low_cardinality_exchange_dictionary(dict_type) => {
                return compute_crc32_hash_dictionary_array(array);
            }
            _ => {
                return Err(stream_contract(format_args!(
                    "hash_partition: unsupported array type for CRC32 hash: {:?}",
                    array.data_type()
                )));
            }
        }

        Ok(hash_values)
    }

    fn canonicalize_hash_array(array: &ArrayRef) -> ExecutionResult<ArrayRef> {
        match array.data_type() {
            DataType::Int8 | DataType::Int16 | DataType::Int32 => {
                cast(array, &DataType::Int64).map_err(partition_arrow)
            }
            DataType::Float32 => cast(array, &DataType::Float64).map_err(partition_arrow),
            _ => Ok(array.clone()),
        }
    }

    // Partition a chunk based on hash values without converting to rows
    fn take_partition_rows_with<E: From<ExecutionFailure>>(
        source: &Chunk,
        rows: &[u32],
        publish: impl FnOnce(&Chunk, Vec<ArrayRef>, usize) -> Result<Chunk, E>,
    ) -> Result<Chunk, E> {
        let indices = arrow::array::UInt32Array::from(rows.to_vec());
        let indices_ref = Arc::new(indices) as ArrayRef;
        let mut columns = Vec::with_capacity(source.record_batch().num_columns());
        for column in source.record_batch().columns() {
            columns.push(
                take(column.as_ref(), &indices_ref, None)
                    .map_err(partition_arrow)
                    .map_err(E::from)?,
            );
        }
        publish(source, columns, rows.len())
    }

    pub(crate) fn take_partition_rows_created(
        source: &Chunk,
        rows: &[u32],
        factory: &dyn crate::exec::pipeline::operator::InputConstructionFactory,
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> crate::runtime::task::ExecutionResult<Chunk> {
        use crate::exec::pipeline::operator::ConstructionFailure;
        checkpoint()?;
        let owner = factory.begin_input_construction(source)?;
        for layout in [
            std::alloc::Layout::array::<u32>(rows.len()),
            std::alloc::Layout::array::<ArrayRef>(source.columns().len()),
        ] {
            owner.request(layout.map_err(|_| {
                ConstructionFailure::from(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic)
            })?)?;
        }
        owner.observe(
            0,
            &[],
            crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork {
                units: rows.len(),
            },
        )?;
        let output = take_partition_rows_with::<crate::runtime::task::ExecutionFailure>(
            source,
            rows,
            |source, columns, row_count| {
                owner.observe(
                    0,
                    &[],
                    crate::exec::chunk::ArrayNormalizationStep::OpaqueOperationFinished,
                )?;
                let storage = source.source_storage_lifetime_observed::<ConstructionFailure>(
                    owner.storage_lifetime(),
                    |layout| owner.request(layout),
                )?;
                Ok(Chunk::try_new_typed_with_columns_owned_observed::<
                    ConstructionFailure,
                    _,
                >(
                    source.chunk_schema_ref(),
                    columns,
                    row_count,
                    storage,
                    |step| owner.observe(0, &[], step),
                )?)
            },
        )?;
        checkpoint()?;
        Ok(output)
    }

    #[cfg(test)]
    pub fn partition_chunk_by_hash_arrays(
        chunk: &Chunk,
        keys: &[ArrayRef],
        partitions: usize,
        crc: bool,
    ) -> ExecutionResult<Vec<Chunk>> {
        partition_chunk_by_hash_arrays_with(chunk, keys, partitions, crc, |source, rows| {
            take_partition_rows_with(source, rows, |source, columns, _| {
                Chunk::try_new_with_columns(source.chunk_schema_ref(), columns).map_err(Into::into)
            })
        })
    }
    pub(crate) fn partition_chunk_by_hash_arrays_created(
        chunk: &Chunk,
        keys: &[ArrayRef],
        partitions: usize,
        crc: bool,
        factory: &dyn crate::exec::pipeline::operator::InputConstructionFactory,
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<Vec<Chunk>> {
        checkpoint()?;
        let output =
            partition_chunk_by_hash_arrays_with(chunk, keys, partitions, crc, |source, rows| {
                take_partition_rows_created(source, rows, factory, checkpoint)
            })?;
        checkpoint()?;
        Ok(output)
    }
    fn partition_chunk_by_hash_arrays_with(
        chunk: &Chunk,
        partition_arrays: &[ArrayRef],
        num_partitions: usize,
        use_crc32: bool,
        mut take_rows: impl FnMut(&Chunk, &[u32]) -> ExecutionResult<Chunk>,
    ) -> ExecutionResult<Vec<Chunk>> {
        if chunk.is_empty() {
            return Ok(vec![Chunk::default(); num_partitions]);
        }

        check_partition_row_extent(chunk.len())?;
        if num_partitions == 0 {
            return Err(stream_contract(
                "nonempty hash partition requires a destination",
            ));
        }

        let partition_arrays = partition_arrays
            .iter()
            .map(|array| {
                if array.len() != chunk.len() {
                    return Err(stream_contract(format_args!(
                        "hash partition key rows {} differ from input rows {}",
                        array.len(),
                        chunk.len()
                    )));
                }
                canonicalize_hash_array(array)
            })
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

        let mut partition_chunks = Vec::with_capacity(num_partitions);
        for rows in &partition_row_indices {
            if rows.is_empty() {
                partition_chunks.push(Chunk::default());
            } else {
                partition_chunks.push(take_rows(chunk, rows)?);
            }
        }

        Ok(partition_chunks)
    }
}

#[cfg(test)]
pub(crate) use data_stream_sink_hash_partition::partition_chunk_by_hash_arrays;
pub(crate) use data_stream_sink_hash_partition::{
    partition_chunk_by_hash_arrays_created, take_partition_rows_created,
};

/// The caller freezes the partition algorithm independently of the legacy
/// carrier spelling. Ordinary keeps the pre-existing exchange mapping.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompiledPartitionRoute {
    Ordinary,
    HierarchicalV1,
    ConditionalKeyV1,
}

/// The partition and optional applicability roots of one exact stream branch.
#[derive(Clone)]
pub(crate) struct CompiledPartitionKeys {
    program: Arc<LocalProgram>,
    sites: Vec<ProgramExpressionRootSite>,
    route: CompiledPartitionRoute,
    applies: Option<ProgramExpressionRootSite>,
}

impl CompiledPartitionKeys {
    pub(crate) fn new(program: Arc<LocalProgram>, sites: Vec<ProgramExpressionRootSite>) -> Self {
        Self {
            program,
            sites,
            route: CompiledPartitionRoute::Ordinary,
            applies: None,
        }
    }

    pub(crate) fn hierarchical(
        program: Arc<LocalProgram>,
        sites: Vec<ProgramExpressionRootSite>,
    ) -> Self {
        Self {
            program,
            sites,
            route: CompiledPartitionRoute::HierarchicalV1,
            applies: None,
        }
    }

    pub(crate) fn conditional(
        program: Arc<LocalProgram>,
        sites: Vec<ProgramExpressionRootSite>,
        applies: ProgramExpressionRootSite,
    ) -> Self {
        Self {
            program,
            sites,
            route: CompiledPartitionRoute::ConditionalKeyV1,
            applies: Some(applies),
        }
    }
}

/// Each driver owns its real compiled root instances. The action root is not
/// evaluated again while keys and routes consume the selected row mapping.
struct CompiledPartitionState {
    keys: CompiledPartitionKeys,
    instances: Option<Vec<CompiledExpressionInstance>>,
    applicability_instances: Option<Vec<CompiledExpressionInstance>>,
}

/// Conditional arrays have one element per TRUE row; selected_rows maps that
/// compact domain back to original payload rows. FALSE/NULL rows never enter
/// a key expression or key decoder. The original creation owner keeps all
/// temporary vectors charged through routing and publication of the takes.
struct CompiledPartitionBatch {
    arrays: Vec<ArrayRef>,
    types: Vec<novarocks_type_contract::FunctionValueType>,
    selected_rows: Option<Vec<usize>>,
    route: CompiledPartitionRoute,
    owner: Box<dyn crate::exec::pipeline::operator::InputConstructionOwner>,
}

fn request_partition_vector<T>(
    owner: &dyn crate::exec::pipeline::operator::InputConstructionOwner,
    count: usize,
) -> ExecutionResult<()> {
    owner.request(std::alloc::Layout::array::<T>(count).map_err(|_| {
        crate::exec::pipeline::operator::ConstructionFailure::from(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
        )
    })?)?;
    Ok(())
}

/// The SQL action decision has already become one Boolean value per row.
fn selected_partition_rows(
    mask: &BooleanArray,
    payload_rows: usize,
    owner: &dyn crate::exec::pipeline::operator::InputConstructionOwner,
    ctx: &DriverStep<'_>,
) -> ExecutionResult<Vec<usize>> {
    if mask.len() != payload_rows {
        return Err(stream_contract(
            "conditional exchange applicability row count differs from payload",
        ));
    }
    request_partition_vector::<usize>(owner, payload_rows)?;
    let mut rows = Vec::with_capacity(payload_rows);
    for row in 0..payload_rows {
        if row.is_multiple_of(256) {
            ctx.checkpoint()?;
        }
        if !mask.is_null(row) && mask.value(row) {
            rows.push(row);
        }
    }
    Ok(rows)
}

/// Form disjoint original-row selections using the shared canonical tuple
/// author and this level's frozen domain. Bypass is per row and per producer,
/// and advances only for FALSE/NULL applicability rows across batch boundaries.
fn instance_partition_rows(
    keys: &CompiledPartitionBatch,
    source: &Chunk,
    input_rows: usize,
    destinations: usize,
    bypass_cursor: &mut usize,
    ctx: &mut DriverStep<'_>,
) -> ExecutionResult<Vec<Vec<u32>>> {
    use crate::exec::hash_table::partition_keys::{
        native_tuple_hashes_observed, require_non_null_keys,
    };
    use novarocks_type_contract::PartitionHashDomain;
    use novarocks_value_arrays::reachability::ReachableRows;
    ctx.checkpoint()?;
    check_partition_row_extent(input_rows)?;
    if destinations == 0 || keys.route == CompiledPartitionRoute::Ordinary {
        return Err(stream_contract(
            "hierarchical exchange requires destinations and an explicit route",
        ));
    }
    let conditional = keys.route == CompiledPartitionRoute::ConditionalKeyV1;
    if conditional != keys.selected_rows.is_some() {
        return Err(stream_contract(
            "conditional exchange selection differs from its route",
        ));
    }
    if let Some(rows) = &keys.selected_rows
        && (rows.last().is_some_and(|row| *row >= input_rows)
            || rows.windows(2).any(|pair| pair[0] >= pair[1]))
    {
        return Err(stream_contract(
            "conditional exchange rows must be ordered within the payload",
        ));
    }
    let active_rows = keys.selected_rows.as_ref().map_or(input_rows, Vec::len);
    if keys.arrays.len() != keys.types.len()
        || (active_rows != 0 && keys.arrays.is_empty())
        || keys.arrays.iter().any(|array| array.len() != active_rows)
    {
        return Err(stream_contract(
            "compiled exchange key columns differ from their selected row domain",
        ));
    }
    let source_owner =
        crate::exec::hash_table::partition_keys::source_key_owner(source, keys.owner.as_ref())?;
    let owner = source_owner.as_ref();
    let (control, conversions) = ctx.value_conversion_scope();
    let checkpoint = || match control.error() {
        Some(original) => Err(original),
        None if control.is_stopped() => Err(novarocks_functions::KernelFailure::Cancelled.into()),
        None => Ok(()),
    };
    let hashes = if active_rows == 0 {
        None
    } else {
        let selected = ReachableRows::all(active_rows);
        if conditional {
            require_non_null_keys(&keys.arrays, &keys.types, &selected, &checkpoint)?;
        }
        Some(native_tuple_hashes_observed(
            &keys.arrays,
            &keys.types,
            &selected,
            conversions,
            owner,
            &checkpoint,
        )?)
    };
    request_partition_vector::<usize>(owner, input_rows)?;
    request_partition_vector::<usize>(owner, destinations)?;
    request_partition_vector::<Vec<u32>>(owner, destinations)?;
    let mut assignment = Vec::with_capacity(input_rows);
    let mut counts = vec![0usize; destinations];
    let mut next_selected = 0usize;
    for row in 0..input_rows {
        if row.is_multiple_of(256) {
            checkpoint()?;
        }
        let selected_index = match &keys.selected_rows {
            None => Some(row),
            Some(rows) if rows.get(next_selected) == Some(&row) => {
                let selected = next_selected;
                next_selected += 1;
                Some(selected)
            }
            Some(_) => None,
        };
        let destination = if let Some(selected) = selected_index {
            let hash = hashes
                .as_ref()
                .and_then(|batch| batch.as_slice()[selected])
                .ok_or_else(|| stream_contract("selected exchange key has no canonical hash"))?;
            (PartitionHashDomain::InstanceV1.mix(hash) % destinations as u64) as usize
        } else {
            let destination = *bypass_cursor % destinations;
            *bypass_cursor = (destination + 1) % destinations;
            destination
        };
        counts[destination] += 1;
        assignment.push(destination);
    }
    let mut rows = Vec::with_capacity(destinations);
    for count in counts {
        request_partition_vector::<u32>(owner, count)?;
        rows.push(Vec::with_capacity(count));
    }
    for (row, destination) in assignment.into_iter().enumerate() {
        rows[destination]
            .push(u32::try_from(row).map_err(|_| stream_contract("exchange row exceeds UInt32"))?);
    }
    checkpoint()?;
    Ok(rows)
}

/// Factory for distributed stream sinks that serialize and transmit chunks to remote fragment instances.
pub struct DataStreamSinkFactory {
    name: String,
    init_error: Option<ExecutionFailure>,
    input: DataStreamSinkFactoryInput,
    task_key: UniqueId,
    sender_id: Option<i32>,
    transmitter: Arc<dyn ExchangeFrameTransmitter>,
    plan_node_id: i32,
    finish_state: Arc<DataStreamSinkFinishState>,
    send_group: Arc<ExchangeSenderGroup>,
    edge_gates: Option<Arc<ExchangeEdgeGates>>,
    compiled_partition: Option<CompiledPartitionKeys>,
    frozen_output: Option<novarocks_local_program::StaticLayout>,
}

impl DataStreamSinkFactory {
    pub fn new(
        input: DataStreamSinkFactoryInput,
        task_key: UniqueId,
        sender_id: Option<i32>,
        plan_node_id: i32,
        transmitter: Arc<dyn ExchangeFrameTransmitter>,
    ) -> Self {
        // Align with StarRocks FE ExplainAnalyzer: ExchangeSinkOperator uses the *upstream plan node id*
        // (not the destination exchange node id) as `plan_node_id`.
        let name = if plan_node_id >= 0 {
            format!("EXCHANGE_SINK (id={plan_node_id})")
        } else {
            "EXCHANGE_SINK".to_string()
        };
        let init_error = None;

        Self {
            name,
            init_error,
            input,
            task_key,
            sender_id,
            transmitter,
            plan_node_id,
            finish_state: Arc::new(DataStreamSinkFinishState::default()),
            send_group: ExchangeSenderGroup::new(),
            edge_gates: None,
            compiled_partition: None,
            frozen_output: None,
        }
    }

    pub(crate) fn with_frozen_output(
        mut self,
        output: &novarocks_local_program::StaticLayout,
    ) -> Self {
        self.frozen_output = Some(output.clone());
        self
    }

    /// Evaluate this sink's partition keys through compiled roots. The
    /// factory retains the exact frozen key sites.
    pub(crate) fn with_compiled_partition_keys(
        mut self,
        keys: CompiledPartitionKeys,
    ) -> ExecutionResult<Self> {
        if !self.input.output_partition_type.requires_exprs() || keys.sites.is_empty() {
            return Err(stream_contract(format_args!(
                "compiled partition keys do not match {} partitioning",
                self.input.output_partition_type.display_name()
            )));
        }
        let conditional = keys.route == CompiledPartitionRoute::ConditionalKeyV1;
        if conditional != keys.applies.is_some()
            || (keys.route != CompiledPartitionRoute::Ordinary
                && self.input.output_partition_type != DataStreamPartitionType::HashPartitioned)
        {
            return Err(stream_contract(
                "compiled partition algorithm differs from its declared branch",
            ));
        }
        if let Some(applies) = keys.applies
            && root_value_type(&keys.program, applies)?.data_type() != &DataType::Boolean
        {
            return Err(stream_contract(
                "conditional partition applicability must be Boolean",
            ));
        }
        self.compiled_partition = Some(keys);
        Ok(self)
    }

    /// Installs the send permission of this producer's outbound edges.
    ///
    /// Every gated edge starts closed, so nothing is sent on it until the
    /// frontend opens it. Runtime binding requires the complete frozen gate
    /// set, including fixtures that exercise the real stream contract.
    pub fn with_edge_gates(mut self, gates: Arc<ExchangeEdgeGates>) -> Self {
        self.edge_gates = Some(gates);
        self
    }

    /// Whether this sink is bound by a gate set.
    ///
    /// The barrier's behavior is covered at the operator level; this exists so
    /// materialization can be shown to have supplied the gates at all, which
    /// is the failure it actually had.
    #[cfg(test)]
    pub(crate) const fn is_edge_gated(&self) -> bool {
        self.edge_gates.is_some()
    }

    /// The outbound edges a normal downstream cancellation closed, lowest id
    /// first, so a status producer can report normal downstream cancellation
    /// rather than a failure. Empty for an ungated sink.
    pub fn normally_canceled_edges(&self) -> Vec<ExchangeEdgeId> {
        self.edge_gates
            .as_ref()
            .map(|gates| gates.normally_canceled_edges())
            .unwrap_or_default()
    }

    /// How many outbound edges a normal downstream cancellation closed.
    pub fn normally_canceled_edge_count(&self) -> usize {
        self.edge_gates
            .as_ref()
            .map_or(0, |gates| gates.normally_canceled_edge_count())
    }
}

impl OperatorFactory for DataStreamSinkFactory {
    #[cfg(test)]
    fn is_edge_gated(&self) -> bool {
        Self::is_edge_gated(self)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, dop: i32, driver_id: i32) -> Box<dyn Operator> {
        use tracing::debug;

        let sender_id = self
            .sender_id
            .unwrap_or((self.task_key.low() as i32) & 0x7fffffff);
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
                .map(|d| format_uuid(d.task_key().high(), d.task_key().low()))
                .collect::<Vec<_>>()
                .join(",");
            debug!(
                "DataStreamSink created: task_key={} plan_node_id={} dest_node_id={} part_type={} dop={} sender_id={} be_number={} destinations={} dest_preview=[{}]",
                format_uuid(self.task_key.high(), self.task_key.low()),
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

        let init_error = self.init_error.clone();

        let send_observable = Arc::clone(self.send_group.observable());

        Box::new(Transform::hydrating(DataStreamSinkOperator {
            name: self.name.clone(),
            input: self.input.clone(),
            task_key: self.task_key,
            transmitter: Arc::clone(&self.transmitter),
            init_error,
            driver_id,
            sender_id,
            be_number,
            send_group: Arc::clone(&self.send_group),
            random_next: 0,
            pending_per_dest: Vec::new(),
            pending_bytes_per_dest: Vec::new(),
            pending_payloads_per_dest: Vec::new(),
            // Replaced at runtime binding with the host-frozen value.
            max_transmit_batched_bytes: 1,
            finished: AtomicBool::new(false),
            aborting: AtomicBool::new(false),
            finishing: AtomicBool::new(false),
            finish_counted: AtomicBool::new(false),
            finish_completed_set: AtomicBool::new(false),
            end_of_stream_sent: AtomicBool::new(false),
            send_observable,
            construction_factory: None,
            frozen_output: self.frozen_output.clone(),
            recipe: None,
            prepared: false,
            exiting: false,
            compiled_control: None,
            error_state: None,
            #[cfg(novarocks_dev_controls)]
            writer_diagnostics: None,
            finish_state: Arc::clone(&self.finish_state),
            profile_initialized: false,
            profiles: None,
            pending_chunks_mem_tracker: None,
            pending_payload_mem_tracker: None,
            retained_payload_mem_tracker: None,
            edge_gates: self.edge_gates.clone(),
            compiled_partition: self.compiled_partition.clone().map(|keys| {
                CompiledPartitionState {
                    keys,
                    instances: None,
                    applicability_instances: None,
                }
            }),
        }))
    }

    fn is_sink(&self) -> bool {
        true
    }
}

struct PendingPayload {
    be_number: i32,
    payload: ExchangeRetainedPayload,
    payload_bytes: usize,
    eos: bool,
}

enum PayloadEnqueue {
    Enqueued,
    NoCapacity(PendingPayload),
    /// The destination's edge was closed by a normal downstream cancellation,
    /// so the payload is dropped rather than parked or sent.
    Discarded,
}

struct DataStreamSinkOperator {
    name: String,
    input: DataStreamSinkFactoryInput,
    task_key: UniqueId,
    transmitter: Arc<dyn ExchangeFrameTransmitter>,
    init_error: Option<ExecutionFailure>,
    driver_id: i32,
    sender_id: i32,
    be_number: i32,
    send_group: Arc<ExchangeSenderGroup>,
    random_next: usize,
    pending_per_dest: Vec<VecDeque<Chunk>>,
    pending_bytes_per_dest: Vec<usize>,
    pending_payloads_per_dest: Vec<Option<PendingPayload>>,
    max_transmit_batched_bytes: usize,
    finished: AtomicBool,
    aborting: AtomicBool,
    finishing: AtomicBool,
    /// Whether this driver's own finish has already been counted against the
    /// sink's driver set.
    ///
    /// `set_finishing` is retried by the driver for as long as
    /// `finishing_wait` says this operator is still waiting -- which is
    /// exactly what happens while the outbound edge has not been opened yet.
    /// Counting on every retry would let one driver consume the whole set and
    /// latch the sink's one-shot end-of-stream guard, after which no driver
    /// could ever emit end-of-stream and the receiver would wait forever.
    finish_counted: AtomicBool,
    /// Whether this driver was the one that completed the set, remembered so
    /// the retries after the edge opens still know who owes end-of-stream.
    finish_completed_set: AtomicBool,
    /// Whether this driver has handed its destinations the end-of-stream
    /// marker.
    ///
    /// It has to be a latch, because nothing else this operator can read tells
    /// the two sides of that moment apart: "every edge is open and nothing is
    /// parked" is equally true just before the marker is enqueued and just
    /// after. A sink that judged itself finished from those two facts alone
    /// latched `finished` in the window between its edge opening and the
    /// driver's next `set_finishing`, and `set_finishing` then returns early on
    /// `finished` -- so the marker was never sent and the receiver counted
    /// senders until the statement deadline.
    end_of_stream_sent: AtomicBool,
    send_observable: Arc<Observable>,
    error_state: Option<Arc<RuntimeErrorState>>,
    #[cfg(novarocks_dev_controls)]
    writer_diagnostics: Option<crate::runtime::writer_diagnostics::WriterDiagnostics>,
    finish_state: Arc<DataStreamSinkFinishState>,
    profile_initialized: bool,
    profiles: Option<crate::runtime::profile::OperatorProfiles>,
    pending_chunks_mem_tracker: Option<Arc<MemTracker>>,
    pending_payload_mem_tracker: Option<Arc<MemTracker>>,
    retained_payload_mem_tracker: Option<Arc<MemTracker>>,
    edge_gates: Option<Arc<ExchangeEdgeGates>>,
    compiled_partition: Option<CompiledPartitionState>,
    construction_factory:
        Option<Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>>,
    frozen_output: Option<novarocks_local_program::StaticLayout>,
    recipe: Option<Arc<crate::runtime::exchange::online_unit::ExchangeRecipe>>,
    prepared: bool,
    exiting: bool,
    compiled_control: Option<RuntimeKernelControl>,
}

impl OperatorLifecycle for DataStreamSinkOperator {
    fn set_construction_factory(
        &mut self,
        factory: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
    ) {
        self.construction_factory = Some(factory);
    }
    fn creates_typed_storage(&self) -> bool {
        true
    }

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
            payload.payload.transfer_to(Arc::clone(&pending_payloads));
        }

        let send_queue = MemTracker::new_child("RetainedPayloads", &tracker);
        self.retained_payload_mem_tracker = Some(send_queue);
    }

    fn set_profiles(&mut self, profiles: crate::runtime::profile::OperatorProfiles) {
        self.send_group.install_profiles(profiles.clone());
        self.profiles = Some(profiles);
    }

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.error_state = Some(state.error_state());
        #[cfg(novarocks_dev_controls)]
        {
            self.writer_diagnostics =
                crate::runtime::writer_diagnostics::WriterDiagnostics::from_runtime(state);
        }
        let mut control = RuntimeKernelControl::new(state.error_state());
        control.bind_runtime_memory(state);
        if let Some(tracker) = &self.pending_chunks_mem_tracker {
            control.bind_mem_tracker(tracker.clone());
        }
        if let Some(factory) = &self.construction_factory {
            control.bind_construction_factory(factory.clone());
        }
        self.compiled_control = Some(control);
        self.be_number = state.backend_num().unwrap_or(0);
        let runtime = state
            .execution_runtime()
            .ok_or(crate::runtime::runtime_state::RuntimeBindingFailure::MissingExecutionRuntime)?;
        self.max_transmit_batched_bytes = runtime.config().exchange_max_transmit_batched_bytes;
        let gates = self
            .edge_gates
            .clone()
            .ok_or_else(|| stream_contract("exchange sender has no frozen Installed barrier"))?;
        let configs = self.destinations().iter()
            .filter(|destination| !Self::is_pseudo_destination(destination))
            .map(|destination| Ok(ExchangeStreamSenderConfig {
                stream: self.stream_for_destination(destination)?,
                destination: destination.endpoint().clone(),
                sender_count: destination.sender_count(),
                sender_id: self.sender_id,
                backend_number: self.be_number,
                delivery: self.input.delivery,
                retained_window: runtime.exchange_flow_limits().stream_window(),
                max_unit_bytes: novarocks_execution_contract::native_result_support::NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES,
                retry_delay: std::time::Duration::from_millis(10),
            }))
            .collect::<ExecutionResult<Vec<_>>>()?;
        self.send_group.bind(
            state.error_state(),
            configs,
            self.transmitter.clone(),
            runtime.exchange_unit_scheduler(),
            gates,
        )?;
        Ok(())
    }

    fn prepare(&mut self) -> ExecutionResult<()> {
        if self.prepared {
            return Ok(());
        }
        let factory = self
            .construction_factory
            .as_ref()
            .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?;
        let output = self
            .frozen_output
            .as_ref()
            .ok_or_else(|| stream_contract("exchange sender has no frozen output declaration"))?;
        let schema = factory.create_compiled_chunk_schema(output)?;
        self.recipe = Some(
            crate::runtime::exchange::online_unit::prepare_recipe(
                &schema,
                &self.input.output_columns,
                factory.as_ref(),
            )
            .map_err(|error| error.into_execution())?,
        );
        self.prepared = true;
        // Align with StarRocks: count actual sink drivers prepared, not planned DOP.
        self.finish_state.register_driver();
        // The factory's shared stream owner registers the one gate observable
        // while binding. Every driver observes that same state transition.
        tracing::debug!(
            "DataStreamSink registered driver: task_key={} driver_id={} dest_node_id={} sender_id={} remaining_drivers={}",
            format_uuid(self.task_key.high(), self.task_key.low()),
            self.driver_id,
            self.input.dest_node_id,
            self.sender_id,
            self.finish_state.remaining_drivers.load(Ordering::SeqCst)
        );
        Ok(())
    }

    #[cfg(test)]
    fn pending_finish(&self) -> Option<crate::exec::pipeline::operator::FinishWatch> {
        (!self.send_group.is_idle()).then(|| {
            crate::exec::pipeline::operator::FinishWatch::Notify(Arc::clone(&self.send_observable))
        })
    }

    fn begin_exit(&mut self) -> ExecutionResult<()> {
        #[cfg(novarocks_dev_controls)]
        self.emit_writer_diagnostic("sink_begin_exit");
        self.exiting = true;
        if !self.finishing.load(Ordering::Acquire) && !self.aborting.load(Ordering::Acquire) {
            self.cancel();
        }
        Ok(())
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.begin_exit()
    }
    fn observe_exit(&mut self) -> ExitObservation {
        #[cfg(novarocks_dev_controls)]
        self.emit_writer_diagnostic("sink_observe_exit");
        if (self.exiting || self.aborting.load(Ordering::Acquire)) && !self.send_group.is_idle() {
            ExitObservation::pending(FinishWatch::Notify(self.send_observable.clone()))
        } else {
            ExitObservation::exited()
        }
    }
    fn cancel(&mut self) {
        #[cfg(novarocks_dev_controls)]
        self.emit_writer_diagnostic("sink_cancel_enter");
        if self.aborting.swap(true, Ordering::AcqRel) {
            return;
        }
        for pending in &mut self.pending_per_dest {
            pending.clear();
        }
        self.pending_bytes_per_dest.fill(0);
        for pending in &mut self.pending_payloads_per_dest {
            *pending = None;
        }
        self.send_group.cancel();
        #[cfg(novarocks_dev_controls)]
        self.emit_writer_diagnostic("sink_cancel_after_stop");
        self.send_observable.defer_notify().arm();
    }
}

impl DataStreamSinkOperator {
    #[cfg(novarocks_dev_controls)]
    fn emit_writer_diagnostic(&self, event: &str) {
        if let Some(scope) = &self.writer_diagnostics {
            scope.emit(event, || format!(
                "task_key={:?} driver_id={} dest_node={} finishing={} aborting={} exiting={} finished={} finish_counted={} last_driver={} remaining_drivers={} end_sent={} pending_chunks={} pending_payloads={} sender_state={}",
                self.task_key, self.driver_id, self.input.dest_node_id,
                self.finishing.load(Ordering::Acquire), self.aborting.load(Ordering::Acquire), self.exiting,
                self.finished.load(Ordering::Acquire), self.finish_counted.load(Ordering::Acquire),
                self.finish_completed_set.load(Ordering::Acquire), self.finish_state.remaining_drivers.load(Ordering::Acquire),
                self.end_of_stream_sent.load(Ordering::Acquire), self.pending_chunk_count_total(), self.pending_payload_count(),
                self.send_group.try_diagnostic_state().unwrap_or_else(|| "unavailable".into())
            ));
        }
    }
    fn stream_for_destination(
        &self,
        destination: &TaskDestination,
    ) -> ExecutionResult<novarocks_execution_contract::ExchangeStreamIdentity> {
        if destination.source_task_key() != self.task_key {
            return Err(stream_contract(
                "exchange destination has another source fragment instance",
            ));
        }
        let source = self
            .error_state
            .as_ref()
            .and_then(|state| state.task_identity())
            .ok_or_else(|| {
                stream_contract("exchange sender requires its original Task identity")
            })?;
        let target = destination.task_identity().ok_or_else(|| {
            stream_contract("exchange destination requires its frozen Task identity")
        })?;
        novarocks_execution_contract::ExchangeStreamIdentity::try_new(
            novarocks_execution_contract::ExchangeSource::new(
                source,
                destination.source_task_key(),
                destination.sender_ordinal(),
            ),
            target,
            *destination.task_key(),
            novarocks_execution_contract::FragmentNodeId::new(self.input.dest_node_id),
        )
        .map_err(stream_contract)
    }

    fn pending_chunk_bytes_total(&self) -> usize {
        self.pending_bytes_per_dest.iter().sum()
    }

    fn pending_chunk_count_total(&self) -> usize {
        self.pending_per_dest.iter().map(VecDeque::len).sum()
    }

    /// The gate of one destination's edge.
    ///
    /// `Ok(None)` means this sink is ungated. An installed gate set that does
    /// not know a destination is a topology error, never an implicit
    /// permission: each destination belongs to exactly one edge, and that is
    /// what keeps a decision about one edge away from another's destinations.
    fn destination_gate(
        &self,
        dest: &TaskDestination,
    ) -> ExecutionResult<Option<Arc<EdgeSendGate>>> {
        let Some(gates) = self.edge_gates.as_ref() else {
            return Ok(None);
        };
        let key = ExchangeDestinationKey::new(*dest.task_key(), self.input.dest_node_id);
        gates
            .gate_for_destination(key)
            .cloned()
            .map(Some)
            .ok_or_else(|| {
                stream_contract(format_args!(
                    "exchange destination {key} is not attributed to any outbound edge of this producer"
                ))
            })
    }

    /// The send state of the destination at `idx` for the read-only
    /// predicates. A destination the gates do not know reports `None`; the
    /// enqueue path is where that becomes an explicit error.
    fn destination_state_at(&self, idx: usize) -> Option<EdgeSendState> {
        let dest = self.destinations().get(idx)?;
        self.destination_gate(dest).ok()?.map(|gate| gate.state())
    }

    /// Whether the destination at `idx` abandoned its frames through a normal
    /// downstream cancellation. Such data is discarded, so it must never hold
    /// this sink open.
    fn destination_withdrawn(&self, idx: usize) -> bool {
        self.destination_state_at(idx) == Some(EdgeSendState::NormallyCanceled)
    }

    /// Whether any destination this sink still owes output to is waiting for
    /// its edge to be opened.
    ///
    /// A producer whose input drains before the frontend opens its edge is a
    /// normal race, not a failure: the open follows one round trip after every
    /// destination acknowledges creation, and a small fragment can finish
    /// reading in less than that. So finishing waits here rather than failing,
    /// and the driver retries once the edge opens. A blocked driver is polled,
    /// so the wait is bounded even without a wake-up.
    fn awaits_edge_permission(&self) -> bool {
        self.input
            .destinations
            .iter()
            .enumerate()
            .any(|(idx, dest)| {
                !Self::is_pseudo_destination(dest)
                    && self.destination_state_at(idx) == Some(EdgeSendState::AwaitingPermission)
            })
    }

    fn has_pending_chunks(&self) -> bool {
        self.pending_per_dest
            .iter()
            .enumerate()
            .any(|(idx, chunks)| !chunks.is_empty() && !self.destination_withdrawn(idx))
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
        self.pending_payloads_per_dest
            .iter()
            .enumerate()
            .any(|(idx, payload)| payload.is_some() && !self.destination_withdrawn(idx))
    }

    fn has_pending_data(&self) -> bool {
        self.has_pending_chunks() || self.has_pending_payloads()
    }

    /// Whether this driver still owes its destinations the end-of-stream
    /// marker.
    ///
    /// Only the driver that completed the sink's driver set sends it, so only
    /// that driver can owe it; every other driver's `finish_completed_set` is
    /// false and this is false with it.
    ///
    /// This is output the sink owes exactly like a parked payload, and it is
    /// the one piece of owed output that cannot be derived from edge state or
    /// from what is buffered: an open edge with nothing parked describes both
    /// "the marker has not been sent yet" and "the marker is sent and this sink
    /// is done".
    fn owes_end_of_stream(&self) -> bool {
        self.finish_completed_set.load(Ordering::SeqCst)
            && !self.end_of_stream_sent.load(Ordering::SeqCst)
    }

    fn pending_payloads_can_send(&self) -> bool {
        for (idx, payload) in self.pending_payloads_per_dest.iter().enumerate() {
            let Some(payload) = payload.as_ref() else {
                continue;
            };
            match self.destination_state_at(idx) {
                // A payload parked on an edge that has no send permission yet
                // is the whole of a closed edge's backpressure: it keeps its
                // single per-destination slot and the sink stops taking input.
                Some(EdgeSendState::AwaitingPermission) => return false,
                // A withdrawn edge's payload is dropped on the next flush, so
                // it must not hold this sink back.
                Some(EdgeSendState::NormallyCanceled) => continue,
                None | Some(EdgeSendState::Open) => {}
            }
            let destination = &self.destinations()[idx];
            let Ok(stream) = self.stream_for_destination(destination) else {
                return true; // A real publication surfaces the typed binding error.
            };
            if !payload.eos && !self.send_group.can_publish(stream) {
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

    fn log_need_input_blocked(&self, reason: &str) {
        if !should_log_need_input() {
            return;
        }
        use tracing::debug;
        let retained_bytes = self.send_group.retained_payload_bytes();
        debug!(
            "DataStreamSink need_input blocked: reason={} task_key={} driver_id={} sender_id={} pending_chunk_bytes={} pending_chunks={} pending_payloads={} pending_payload_bytes={} retained_payload_bytes={} finishing={} send_idle={}",
            reason,
            format_uuid(self.task_key.high(), self.task_key.low()),
            self.driver_id,
            self.sender_id,
            self.pending_chunk_bytes_total(),
            self.pending_chunk_count_total(),
            self.pending_payload_count(),
            self.pending_payload_bytes_total(),
            retained_bytes,
            self.finishing.load(Ordering::Acquire),
            self.send_group.is_idle()
        );
    }

    fn current_error(&self) -> Option<ExecutionFailure> {
        if let Some(error) = self.error_state.as_ref().and_then(|state| state.error()) {
            return Some(error);
        }
        if let Some(error) = self.send_group.failure() {
            return Some(error);
        }
        if self.input.delivery
            == novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain
            && !self.aborting.load(Ordering::Acquire)
            && self.destinations().iter().enumerate().any(|(i, d)| {
                !Self::is_pseudo_destination(d)
                    && self.destination_withdrawn(i)
                    && self
                        .stream_for_destination(d)
                        .is_ok_and(|stream| !self.send_group.has_end(stream))
            })
        {
            return Some(stream_contract(
                "required exchange input withdrew before its End was delivered",
            ));
        }
        None
    }

    fn maybe_mark_finished(&self) -> bool {
        if self.current_error().is_some() {
            return false;
        }
        if self.finished.load(Ordering::Acquire) {
            return true;
        }
        if self.aborting.load(Ordering::Acquire) {
            if self.send_group.is_idle() {
                self.finished.store(true, Ordering::Release);
                return true;
            }
            return false;
        }
        // A destination still awaiting permission is owed its end-of-stream
        // even when no data is parked for it. Finishing without that marker
        // would leave the destination counting senders forever.
        //
        // `owes_end_of_stream` is the same rule for the moment after the edge
        // opens. This predicate is read by `is_finished` and `need_input`,
        // which the scheduler calls between driver turns, so without it the
        // sink declares itself finished as soon as its edge opens -- before the
        // driver has had a turn in which to send the marker, and `set_finishing`
        // returns early once `finished` is latched.
        // A drained buffer is not a completed driver. `set_finishing` may have
        // returned while that buffer was still parked, before this driver
        // could be counted. If a later readiness check finishes it here, the
        // shared count never reaches zero and no driver sends EOS.
        if self.finishing.load(Ordering::Acquire)
            && self.finish_counted.load(Ordering::Acquire)
            && !self.has_pending_data()
            && !self.awaits_edge_permission()
            && !self.owes_end_of_stream()
            && self.send_group.is_idle()
            && self.current_error().is_none()
        {
            if !self.finished.swap(true, Ordering::AcqRel) {
                // The one line that says this sink stopped owing output, and
                // on whose strength the driver terminates. Absent it, a sink
                // that finished with its seal unsent is indistinguishable
                // from one that never got that far.
                tracing::debug!(
                    "DataStreamSink finished: task_key={} driver_id={} dest_node_id={} sender_id={} end_of_stream_sent={} last_driver={}",
                    format_uuid(self.task_key.high(), self.task_key.low()),
                    self.driver_id,
                    self.input.dest_node_id,
                    self.sender_id,
                    self.end_of_stream_sent.load(Ordering::SeqCst),
                    self.finish_completed_set.load(Ordering::SeqCst),
                );
            }
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

    fn destinations(&self) -> &[TaskDestination] {
        &self.input.destinations
    }

    /// Returns true if the destination at index `i` is a pseudo (pruned-bucket) destination.
    ///
    /// In bucket shuffle joins, FE creates one destination per bucket of the probe-side table.
    /// When predicates prune away all tablets in some buckets, FE still emits destination entries
    /// to keep the bucket index ↔ destination index mapping correct for the hash partitioner,
    /// but marks them with `task_key.lo == -1` and an invalid (zero) port.
    /// No fragment instance runs for these buckets, so the sender must skip them entirely:
    /// no data buffering, no serialization, no EOS packet.
    /// StarRocks BE applies the same `lo == -1` check in DataStreamSender::send_chunk(),
    /// ExchangeSinkOperator::push_chunk(), and Channel::close().
    fn is_pseudo_destination(dest: &TaskDestination) -> bool {
        dest.task_key().low() == -1
    }

    /// Evaluate applicability once over the original payload. Only its TRUE
    /// rows demand key roots; selected INSERT and no-match rows keep their
    /// original payload position without reading a target key.
    fn compiled_partition_keys(
        &mut self,
        ctx: &DriverStep<'_>,
        chunk: &Chunk,
    ) -> ExecutionResult<Option<CompiledPartitionBatch>> {
        let Some(state) = self.compiled_partition.as_mut() else {
            return Ok(None);
        };
        ctx.checkpoint()?;
        let control = self
            .compiled_control
            .as_ref()
            .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?;
        let owner = self
            .construction_factory
            .as_deref()
            .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?
            .begin_input_construction(chunk)?;
        let selected_rows = if let Some(applies) = state.keys.applies {
            compiled_instances(
                &mut state.applicability_instances,
                &state.keys.program,
                &[applies],
                control,
            )?;
            let instance = state
                .applicability_instances
                .as_mut()
                .and_then(|items| items.first_mut())
                .ok_or_else(|| {
                    stream_contract("conditional exchange applicability instance is absent")
                })?;
            let array = evaluate_all(instance, applies, chunk.record_batch(), control)?;
            let mask = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    stream_contract("conditional exchange applicability is not Boolean")
                })?;
            Some(selected_partition_rows(
                mask,
                chunk.len(),
                owner.as_ref(),
                ctx,
            )?)
        } else {
            None
        };
        let active = selected_rows.as_ref().is_none_or(|rows| !rows.is_empty());
        request_partition_vector::<ArrayRef>(owner.as_ref(), state.keys.sites.len())?;
        request_partition_vector::<novarocks_type_contract::FunctionValueType>(
            owner.as_ref(),
            state.keys.sites.len(),
        )?;
        let mut arrays = Vec::with_capacity(state.keys.sites.len());
        let mut types = Vec::with_capacity(state.keys.sites.len());
        if active {
            compiled_instances(
                &mut state.instances,
                &state.keys.program,
                &state.keys.sites,
                control,
            )?;
            let instances = state
                .instances
                .as_mut()
                .ok_or_else(|| stream_contract("compiled partition key instances are absent"))?;
            for (instance, site) in instances.iter_mut().zip(&state.keys.sites) {
                let ty = root_value_type(&state.keys.program, *site)?;
                let array = match &selected_rows {
                    Some(rows) => {
                        evaluate_selected(instance, *site, chunk.record_batch(), rows, control)?
                    }
                    None => evaluate_all(instance, *site, chunk.record_batch(), control)?,
                };
                // Preserve the ordinary path's exact historical refusal.
                // New routes use the signed logical type rather than guessing
                // VARIANT from a plain binary carrier.
                if (state.keys.route == CompiledPartitionRoute::Ordinary
                    && matches!(array.data_type(), DataType::LargeBinary))
                    || ty.logical_type() == novarocks_type_contract::ValueLogicalType::Variant
                {
                    return Err(stream_contract(
                        "VARIANT is not supported in HASH_PARTITIONED partition keys",
                    ));
                }
                arrays.push(array);
                types.push(ty);
            }
        }
        ctx.checkpoint()?;
        Ok(Some(CompiledPartitionBatch {
            arrays,
            types,
            selected_rows,
            route: state.keys.route,
            owner,
        }))
    }

    fn partition_chunk(
        &mut self,
        ctx: &mut DriverStep<'_>,
        chunk: &Chunk,
        compiled_keys: Option<&CompiledPartitionBatch>,
    ) -> ExecutionResult<Vec<Vec<Chunk>>> {
        // Get destinations first to avoid borrowing self
        let dests: Vec<TaskDestination> = self.destinations().to_vec();
        if dests.is_empty() {
            return Ok(vec![]);
        }

        if chunk.is_empty() {
            return Ok(vec![Vec::new(); dests.len()]);
        }

        let n = dests.len();
        if let Some(keys) = compiled_keys
            && keys.route != CompiledPartitionRoute::Ordinary
        {
            let rows =
                instance_partition_rows(keys, chunk, chunk.len(), n, &mut self.random_next, ctx)?;
            let factory = self
                .construction_factory
                .as_deref()
                .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?;
            request_partition_vector::<Vec<Chunk>>(keys.owner.as_ref(), n)?;
            let mut partitioned = Vec::with_capacity(n);
            for selection in rows {
                if selection.is_empty() {
                    partitioned.push(Vec::new());
                } else {
                    request_partition_vector::<Chunk>(keys.owner.as_ref(), 1)?;
                    partitioned.push(vec![take_partition_rows_created(
                        chunk,
                        &selection,
                        factory,
                        &|| ctx.checkpoint(),
                    )?]);
                }
            }
            return Ok(partitioned);
        }

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
                check_partition_row_extent(num_rows)?;
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
                        let factory = self.construction_factory.as_deref().ok_or(
                            crate::exec::pipeline::operator::ConstructionFailure::MissingOwner,
                        )?;
                        let new_chunk =
                            take_partition_rows_created(chunk, row_indices, factory, &|| {
                                ctx.checkpoint()
                            })?;

                        per_dest_chunks.push(vec![new_chunk]);
                    }
                }
                Ok(per_dest_chunks)
            }
            DataStreamPartitionType::HashPartitioned
            | DataStreamPartitionType::BucketShuffleHashPartitioned => {
                // Use vectorized hash partition without row conversion
                let use_crc32 = matches!(
                    self.input.output_partition_type,
                    DataStreamPartitionType::BucketShuffleHashPartitioned
                );

                let keys = compiled_keys.ok_or_else(|| {
                    stream_contract("HASH_PARTITIONED missing compiled partition keys")
                })?;
                let factory = self
                    .construction_factory
                    .as_deref()
                    .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?;
                let partition_chunks = partition_chunk_by_hash_arrays_created(
                    chunk,
                    &keys.arrays,
                    n,
                    use_crc32,
                    factory,
                    &|| ctx.checkpoint(),
                )?;

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
        dest: &TaskDestination,
        pending: PendingPayload,
    ) -> ExecutionResult<PayloadEnqueue> {
        let stream = self.stream_for_destination(dest)?;
        if pending.be_number != self.send_group.backend_number(stream)? {
            return Err(stream_contract(
                "encoded exchange backend number differs from its frozen stream metadata",
            ));
        }
        let gate = self.destination_gate(dest)?;
        match gate.as_ref().map(|gate| gate.state()) {
            Some(EdgeSendState::AwaitingPermission) => {
                return Ok(PayloadEnqueue::NoCapacity(pending));
            }
            Some(EdgeSendState::NormallyCanceled) => {
                if self.input.delivery
                    == novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain
                {
                    if pending.eos && pending.payload_bytes == 0 && self.send_group.has_end(stream)
                    {
                        return Ok(PayloadEnqueue::Enqueued);
                    }
                    return Err(stream_contract(
                        "required exchange destination withdrew before publication",
                    ));
                }
                return Ok(PayloadEnqueue::Discarded);
            }
            Some(EdgeSendState::Open) => {}
            None => {
                return Err(stream_contract(
                    "exchange publication has no frozen destination gate",
                ));
            }
        }
        let outcome = if pending.eos && pending.payload_bytes == 0 {
            self.send_group.finish(stream)?
        } else {
            self.send_group
                .publish(stream, pending.payload.bytes(), pending.eos)?
        };
        match outcome {
            GroupPublication::Backpressured => Ok(PayloadEnqueue::NoCapacity(pending)),
            GroupPublication::Withdrawn => Ok(PayloadEnqueue::Discarded),
            GroupPublication::Published => {
                if let Some(tracker) = self.retained_payload_mem_tracker.as_ref() {
                    pending.payload.transfer_to(tracker.clone());
                }
                #[cfg(test)]
                record_payload_identity_for_test(self.task_key, pending.be_number, pending.eos);
                Ok(PayloadEnqueue::Enqueued)
            }
        }
    }

    fn transmit_partition(
        &mut self,
        _dest_idx: usize,
        dest: &TaskDestination,
        chunks: &[Chunk],
        eos: bool,
    ) -> ExecutionResult<PayloadEnqueue> {
        use tracing::debug;

        self.init_profile_if_needed();

        let dest_finst_id = UniqueId::new(dest.task_key().high(), dest.task_key().low());

        let row_count: usize = chunks.iter().map(|c| c.len()).sum();
        debug!(
            "DataStreamSink::transmit_partition: destination_task_key={} node_id={} sender_id={} chunks={} rows={} eos={}",
            dest_finst_id,
            self.input.dest_node_id,
            self.sender_id,
            chunks.len(),
            row_count,
            eos
        );

        let factory = self
            .construction_factory
            .as_ref()
            .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?;
        let projection_owner;
        let projected_storage;
        let chunks = if self.input.output_columns.is_empty() || chunks.is_empty() {
            projection_owner = None;
            chunks
        } else {
            let source = chunks.first().expect("nonempty branch checked");
            let owner = factory.begin_input_construction(source)?;
            owner.request(std::alloc::Layout::array::<Chunk>(chunks.len()).map_err(|_| crate::exec::pipeline::operator::ConstructionFailure::Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic))?)?;
            let mut projected = Vec::with_capacity(chunks.len());
            for chunk in chunks {
                projected.push(project_chunk_by_slot_ids_with_creation(
                    chunk,
                    &self.input.output_columns,
                    factory.as_ref(),
                )?);
            }
            projection_owner = Some(owner);
            projected_storage = projected;
            projected_storage.as_slice()
        };

        let _projection_owner = projection_owner.as_ref();
        let be_number = self.be_number;
        let encode_start = std::time::Instant::now();
        let error_state = self
            .error_state
            .as_ref()
            .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?;
        let control = RuntimeKernelControl::new(error_state.clone());
        let payload = match chunks {
            [] => Vec::new(),
            [chunk] => crate::runtime::exchange::online_unit::encode(
                self.recipe.as_deref().ok_or_else(|| {
                    stream_contract("exchange sender recipe was not installed during preparation")
                })?,
                chunk,
                factory.as_ref(),
                &control,
            )
            .map_err(|error| error.into_execution())?,
            _ => {
                return Err(stream_contract(
                    "one exchange unit must contain exactly one complete batch",
                ));
            }
        };
        let encode_ns = encode_start.elapsed().as_nanos();
        let payload_bytes = payload.len();
        let payload_capacity_bytes = payload.capacity().max(payload_bytes);

        if let Some(profile) = self.profiles.as_ref() {
            profile.common.counter_add(
                "OverallTime",
                ProfileUnit::TimeNs,
                clamp_u128_to_i64(encode_ns),
            );
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
            payload: ExchangeRetainedPayload::new(payload, accounting),
            payload_bytes,
            eos,
        };

        match self.try_enqueue_payload(dest, payload)? {
            PayloadEnqueue::Enqueued => {
                debug!(
                    "DataStreamSink::transmit_partition enqueued: destination_task_key={} node_id={} eos={} bytes={}",
                    dest_finst_id, self.input.dest_node_id, eos, payload_bytes
                );
                Ok(PayloadEnqueue::Enqueued)
            }
            PayloadEnqueue::Discarded => {
                debug!(
                    "DataStreamSink::transmit_partition discarded: destination_task_key={} node_id={} eos={} bytes={} reason=destination_cancelled_normally",
                    dest_finst_id, self.input.dest_node_id, eos, payload_bytes
                );
                Ok(PayloadEnqueue::Discarded)
            }
            PayloadEnqueue::NoCapacity(payload) => Ok(PayloadEnqueue::NoCapacity(payload)),
        }
    }

    /// Drops everything still owed to one abandoned destination: its buffered
    /// chunks, their byte accounting, and its single parked payload.
    fn discard_pending_for_dest(&mut self, dest_idx: usize) {
        if let Some(chunks) = self.pending_per_dest.get_mut(dest_idx) {
            chunks.clear();
        }
        if let Some(bytes) = self.pending_bytes_per_dest.get_mut(dest_idx) {
            *bytes = 0;
        }
        if let Some(payload) = self.pending_payloads_per_dest.get_mut(dest_idx) {
            *payload = None;
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

    #[cfg(test)]
    fn buffer_chunk(&mut self, chunk: Chunk) -> ExecutionResult<()> {
        let control = self
            .error_state
            .clone()
            .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?;
        let mut work =
            crate::exec::pipeline::driver_step::TurnWork::new(std::num::NonZeroU64::MIN, 64);
        let mut value_conversions =
            novarocks_value_arrays::decode::ValueConversionContext::default();
        let mut ctx = DriverStep::new(&control, &mut work, &mut value_conversions);
        self.buffer_chunk_with_keys(&mut ctx, chunk, None)
    }

    fn buffer_chunk_with_keys(
        &mut self,
        ctx: &mut DriverStep<'_>,
        chunk: Chunk,
        compiled_keys: Option<CompiledPartitionBatch>,
    ) -> ExecutionResult<()> {
        self.ensure_pending_buffers_initialized();
        let dests = self.destinations().to_vec();
        let per_dest_chunks = self.partition_chunk(ctx, &chunk, compiled_keys.as_ref())?;
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
            let factory = self
                .construction_factory
                .as_ref()
                .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?;
            let split =
                split_oversized_chunks_with_creation(chunks, max_chunk_bytes, factory.as_ref())?;
            let mut chunks = split.chunks;
            if let Some(tracker) = self.pending_chunks_mem_tracker.as_ref() {
                for chunk in chunks.iter_mut() {
                    chunk.transfer_to(tracker);
                }
            }
            let bytes = chunks
                .iter()
                .map(|c| c.record_batch().get_array_memory_size())
                .sum::<usize>();
            self.pending_per_dest[i].extend(chunks);
            self.pending_bytes_per_dest[i] = self.pending_bytes_per_dest[i].saturating_add(bytes);
        }
        Ok(())
    }

    fn drain_pending_chunk(&mut self, dest_idx: usize) -> ExecutionResult<Option<Chunk>> {
        let pending = self
            .pending_per_dest
            .get_mut(dest_idx)
            .ok_or_else(|| stream_contract("pending buffer index out of range"))?;
        let Some(chunk) = pending.pop_front() else {
            return Ok(None);
        };
        let bytes = chunk.record_batch().get_array_memory_size();
        if let Some(total) = self.pending_bytes_per_dest.get_mut(dest_idx) {
            *total = total.saturating_sub(bytes);
        }
        // Each original chunk is one independently decodable Unit. Remaining
        // chunks stay queued under the existing sender backpressure owner.
        Ok(Some(chunk))
    }

    fn flush_pending(&mut self, force: bool) -> ExecutionResult<()> {
        self.ensure_pending_buffers_initialized();
        let dests: Vec<TaskDestination> = self.destinations().to_vec();
        for (i, dest) in dests.iter().enumerate() {
            if Self::is_pseudo_destination(dest) {
                continue;
            }
            let send_state = self.destination_state_at(i);
            // A destination that left normally abandons its own edge's frames
            // and nothing else.
            if send_state == Some(EdgeSendState::NormallyCanceled) {
                if self.input.delivery
                    == novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain
                {
                    return Err(stream_contract(
                        "required exchange input withdrew while output was pending",
                    ));
                }
                self.discard_pending_for_dest(i);
                continue;
            }
            // A closed edge must not stall the destinations that can send, so
            // it parks its own payload and the loop moves on to the next
            // destination instead of abandoning the whole flush.
            // A closed edge parks its own payload and lets the loop move on:
            // `try_enqueue_payload` reserves nothing for it, on the finishing
            // drain exactly as on a normal one. The operator then reports
            // finishing as still pending, so the driver comes back instead of
            // treating a legitimate race as a failure.
            let closed_edge = send_state == Some(EdgeSendState::AwaitingPermission);
            let pending_payload = self
                .pending_payloads_per_dest
                .get_mut(i)
                .ok_or_else(|| stream_contract("pending payload index out of range"))?
                .take();
            if let Some(payload) = pending_payload {
                match self.try_enqueue_payload(dest, payload)? {
                    PayloadEnqueue::Enqueued | PayloadEnqueue::Discarded => {}
                    PayloadEnqueue::NoCapacity(payload) => {
                        self.pending_payloads_per_dest[i] = Some(payload);
                        if closed_edge {
                            continue;
                        }
                        return Ok(());
                    }
                }
            }

            loop {
                if !self.should_flush_pending_dest(i, force) {
                    break;
                }
                let Some(chunk) = self.drain_pending_chunk(i)? else {
                    break;
                };
                match self.transmit_partition(i, dest, std::slice::from_ref(&chunk), false)? {
                    PayloadEnqueue::Enqueued => {}
                    PayloadEnqueue::Discarded => {
                        self.discard_pending_for_dest(i);
                        break;
                    }
                    PayloadEnqueue::NoCapacity(payload) => {
                        self.pending_payloads_per_dest[i] = Some(payload);
                        if closed_edge {
                            break;
                        }
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

    /// Hands every destination this sink still owes one the end-of-stream
    /// marker.
    ///
    /// Only the driver that completed the sink's driver set calls it, and only
    /// once every edge is open and nothing is parked -- so a destination
    /// skipped here is skipped for a reason that is permanent (it is a pruned
    /// pseudo destination, or it withdrew), never because it is not ready yet.
    /// That is what makes it sound to latch "the seal is sent" on return.
    fn send_eos(&mut self) -> ExecutionResult<()> {
        // The latched last-driver verdict survives every continuation. Once
        // every exact destination received its marker, no later poll resends it.
        if self.end_of_stream_sent.load(Ordering::Acquire) {
            return Ok(());
        }
        self.ensure_pending_buffers_initialized();
        let dests: Vec<TaskDestination> = self.destinations().to_vec();
        let mut sealed = 0_usize;
        let mut skipped = 0_usize;
        for (i, dest) in dests.iter().enumerate() {
            // No fragment instance is running for pseudo destinations — do not send EOS.
            if Self::is_pseudo_destination(dest) {
                skipped += 1;
                continue;
            }
            match self.destination_state_at(i) {
                // A destination that left normally is not owed an
                // end-of-stream marker either.
                Some(EdgeSendState::NormallyCanceled) => {
                    if self.input.delivery
                        == novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain
                        && !self.send_group.has_end(self.stream_for_destination(dest)?)
                    {
                        return Err(stream_contract(
                            "required exchange input withdrew before End admission",
                        ));
                    }
                    self.discard_pending_for_dest(i);
                    skipped += 1;
                    continue;
                }
                // End-of-stream is output like any other frame, so it needs
                // permission. The caller checks that no edge awaits permission
                // before it calls this, and an edge never returns to awaiting
                // permission once it leaves that state -- so this is
                // unreachable, and it is refused rather than skipped: sealing
                // some destinations and silently not others would latch "the
                // seal is sent" over a sender the receiver keeps counting.
                Some(EdgeSendState::AwaitingPermission) => {
                    return Err(stream_contract(format_args!(
                        "exchange send EOS reached destination {} whose edge is still awaiting permission",
                        dest.task_key()
                    )));
                }
                None | Some(EdgeSendState::Open) => {}
            }
            match self.transmit_partition(i, dest, &[], true)? {
                PayloadEnqueue::Enqueued | PayloadEnqueue::Discarded => sealed += 1,
                PayloadEnqueue::NoCapacity(payload) => {
                    self.pending_payloads_per_dest[i] = Some(payload);
                    return Ok(());
                }
            }
        }
        // Latched only now that every destination this sink owes has its
        // marker: the receiver's sender count closes on this and on nothing
        // else, so the latch has to describe the send that happened rather
        // than the intent to send.
        self.end_of_stream_sent.store(true, Ordering::SeqCst);
        tracing::debug!(
            "DataStreamSink end-of-stream sent: task_key={} driver_id={} dest_node_id={} sender_id={} sealed={} skipped={}",
            format_uuid(self.task_key.high(), self.task_key.low()),
            self.driver_id,
            self.input.dest_node_id,
            self.sender_id,
            sealed,
            skipped,
        );
        Ok(())
    }
}

impl InputEncodingPolicy for DataStreamSinkOperator {
    fn retains_encoded_path(
        &self,
        value: crate::exec::pipeline::operator::EncodedValueOccurrence<'_>,
    ) -> bool {
        if self
            .compiled_partition
            .as_ref()
            .is_some_and(|state| state.keys.route != CompiledPartitionRoute::Ordinary)
        {
            // The selected key evaluator and decoder own reachability. Eager
            // Transform hydration must not inspect excluded dictionary rows.
            return true;
        }
        value.path.is_empty() && self.retains_encoded_column(value.slot, value.carrier)
    }
    fn retains_encoded_column(&self, _slot_id: SlotId, data_type: &DataType) -> bool {
        is_low_cardinality_exchange_dictionary(data_type)
            && matches!(
                self.input.output_partition_type,
                DataStreamPartitionType::Unpartitioned
                    | DataStreamPartitionType::Random
                    | DataStreamPartitionType::HashPartitioned
                    | DataStreamPartitionType::BucketShuffleHashPartitioned
            )
    }
}
impl DataStreamSinkOperator {
    fn input_admissible(&self) -> bool {
        if self.aborting.load(Ordering::Acquire) {
            return false;
        }
        if self.maybe_mark_finished() {
            return false;
        }
        if self.finishing.load(Ordering::Acquire) {
            let ready = !self.has_pending_data() && self.send_group.is_idle();
            if !ready {
                self.log_need_input_blocked("finishing_wait");
            }
            return ready;
        }
        if self.has_pending_payloads() && !self.pending_payloads_can_send() {
            self.log_need_input_blocked("pending_payloads");
            return false;
        }

        true
    }
    fn advance_finish(&mut self) -> ExecutionResult<()> {
        use tracing::debug;
        #[cfg(novarocks_dev_controls)]
        self.emit_writer_diagnostic("sink_advance_finish_enter");

        if self.aborting.load(Ordering::Acquire) {
            return Ok(());
        }

        if let Some(err) = self.init_error.as_ref() {
            return Err(err.clone());
        }
        if self.finished.load(Ordering::Acquire) {
            return Ok(());
        }
        if let Some(err) = self.current_error() {
            return Err(err);
        }
        self.init_profile_if_needed();
        self.finishing.store(true, Ordering::Release);

        debug!(
            "DataStreamSink set_finishing: task_key={} driver_id={} dest_node_id={} sender_id={} be_number={} destinations={} pending_dests={} pending_chunk_bytes_total={} pending_payload_bytes_total={}",
            format_uuid(self.task_key.high(), self.task_key.low()),
            self.driver_id,
            self.input.dest_node_id,
            self.sender_id,
            self.be_number,
            self.destinations().len(),
            self.pending_per_dest.len(),
            self.pending_chunk_bytes_total(),
            self.pending_payload_bytes_total()
        );

        self.flush_pending(true)?;
        // The gate state per outbound edge, and the state of the gate each
        // destination of THIS sink resolves to. A sink that waits forever is
        // waiting on one of these, and without both halves it cannot be told
        // apart from a sink whose edge opened but whose data never drained.
        debug!(
            "DataStreamSink finishing gates: task_key={} driver_id={} dest_node_id={} edges={:?} destination_gates={:?} awaits_permission={} has_pending_data={}",
            format_uuid(self.task_key.high(), self.task_key.low()),
            self.driver_id,
            self.input.dest_node_id,
            self.edge_gates
                .as_ref()
                .map(|gates| gates.edges().collect::<Vec<_>>()),
            self.destinations()
                .iter()
                .enumerate()
                .map(|(idx, dest)| (
                    dest.task_key().low(),
                    Self::is_pseudo_destination(dest),
                    self.destination_state_at(idx)
                ))
                .collect::<Vec<_>>(),
            self.awaits_edge_permission(),
            self.has_pending_data(),
        );
        // A driver holding rows is not finished, whatever the driver set's
        // arithmetic says.
        //
        // The pending buffers are per driver, but the end-of-stream is per
        // sender: whichever driver completes the set sends one marker for all
        // of them, and the receiver's sender count closes on that one marker.
        // A driver that counted itself while still holding rows would let a
        // sibling seal the sender over frames nobody has sent, and the
        // receiver would report the exchange complete and silently short --
        // no error anywhere, just fewer rows.
        //
        // So the count means "this driver has handed over everything it
        // holds", which is what earns the last-driver verdict the right to
        // speak for the others. `finishing_wait` already reports owed output
        // for a driver with pending data and parks one whose edge is still
        // closed, so returning here costs a turn, not the rows. Permission
        // deliberately does not withhold the count: an edge that is closed
        // with nothing behind it owes nothing, and withholding on that would
        // stall the seal on a driver that has no rows to lose.
        if self.has_pending_data() {
            return Ok(());
        }
        // One driver reports its own finish exactly once, however many times
        // the driver retries this call while waiting for its edge to open.
        let is_last_driver = if self.finish_counted.swap(true, Ordering::SeqCst) {
            self.finish_completed_set.load(Ordering::SeqCst)
        } else {
            let completed_set = self.finish_state.driver_finished();
            self.finish_completed_set
                .store(completed_set, Ordering::SeqCst);
            completed_set
        };
        debug!(
            "DataStreamSink finishing progressed: task_key={} driver_id={} dest_node_id={} sender_id={} last_driver={} (only last driver sends EOS)",
            format_uuid(self.task_key.high(), self.task_key.low()),
            self.driver_id,
            self.input.dest_node_id,
            self.sender_id,
            is_last_driver
        );
        if !is_last_driver {
            return Ok(());
        }
        // The marker is output like any other frame, so it needs this
        // driver's edges open. The last-driver verdict is latched, so
        // returning here keeps it for the retry that `finishing_wait` asks
        // for. Pending data needs no second check: no driver reaches the
        // count while it still holds any.
        if self.awaits_edge_permission() {
            return Ok(());
        }
        self.send_eos()?;
        #[cfg(novarocks_dev_controls)]
        self.emit_writer_diagnostic("sink_end_publication_returned");
        self.maybe_mark_finished();
        Ok(())
    }
    fn next_work(&self, observed_generation: u64) -> NextStep {
        if self.aborting.load(Ordering::Acquire) {
            return NextStep::Finished;
        }
        let finishing = self.finishing.load(Ordering::Acquire);
        // A non-last drained driver owes no sender EOS. The last driver keeps
        // the mandatory marker until its exact destinations permit it.
        if finishing
            && self.finish_counted.load(Ordering::Acquire)
            && !self.has_pending_data()
            && !self.owes_end_of_stream()
        {
            return NextStep::Finished;
        }
        let blocked = (finishing && self.awaits_edge_permission())
            || (self.has_pending_payloads() && !self.pending_payloads_can_send());
        if !blocked
            && ((finishing && (self.has_pending_data() || self.owes_end_of_stream()))
                || (!finishing && self.has_pending_payloads() && self.pending_payloads_can_send()))
        {
            return NextStep::Runnable;
        }
        if !finishing && self.input_admissible() {
            return NextStep::NeedInput;
        }
        NextStep::Wait(WaitFact {
            reason: if finishing {
                WaitReason::Finish
            } else {
                WaitReason::Output
            },
            observable: self.send_observable.clone(),
            observed_generation,
            deadline: None,
        })
    }
}
impl SinkKernel for DataStreamSinkOperator {
    fn control_observable(&self) -> Option<Arc<Observable>> {
        Some(self.send_observable.clone())
    }
    fn poll_control(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<ControlStep> {
        if let Some(error) = self.current_error() {
            return Err(error);
        }
        if !self.destinations().is_empty()
            && self.destinations().iter().enumerate().all(|(index, dest)| {
                Self::is_pseudo_destination(dest) || self.destination_withdrawn(index)
            })
        {
            self.cancel();
            return Ok(ControlStep::Withdrawn);
        }
        Ok(ControlStep::Active)
    }
    fn admission(
        &self,
        _: &mut DriverStep<'_>,
        _: &RawInput,
    ) -> ExecutionResult<Option<AdmissionNext>> {
        if !self.prepared {
            return Err(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner.into());
        }
        if let Some(error) = self.current_error() {
            return Err(error);
        }
        let observed_generation = self.send_observable.generation();
        if self.input_admissible() {
            Ok(None)
        } else {
            Ok(Some(AdmissionNext::Wait(WaitFact {
                reason: WaitReason::Output,
                observable: self.send_observable.clone(),
                observed_generation,
                deadline: None,
            })))
        }
    }
    fn consume(
        &mut self,
        _ctx: &mut DriverStep<'_>,
        chunk: PreparedInput,
    ) -> ExecutionResult<SinkStep> {
        let generation = self.send_observable.generation();
        let rows = chunk.chunk().len() as u64;
        let chunk = chunk.into_uncovered_chunk()?;
        if !self.prepared {
            return Err(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner.into());
        }
        if self.aborting.load(Ordering::Acquire) {
            return Err(novarocks_functions::KernelFailure::Cancelled.into());
        }
        if let Some(err) = self.init_error.as_ref() {
            return Err(err.clone());
        }
        if self.finished.load(Ordering::Acquire) {
            return Ok(SinkStep::new(self.next_work(generation), 0));
        }
        if let Some(err) = self.current_error() {
            return Err(err);
        }
        self.init_profile_if_needed();
        self.flush_pending(false)?;
        if chunk.is_empty() {
            return Ok(SinkStep::new(self.next_work(generation), 0));
        }
        let compiled_keys = self.compiled_partition_keys(_ctx, &chunk)?;
        self.buffer_chunk_with_keys(_ctx, chunk, compiled_keys)?;
        self.flush_pending(false)?;
        Ok(SinkStep::new(self.next_work(generation), rows))
    }
    fn poll(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<SinkStep> {
        let generation = self.send_observable.generation();
        if !ctx.work().try_consume(1) {
            return Ok(SinkStep::new(NextStep::Yield, 0));
        }
        if let Some(error) = self.current_error() {
            return Err(error);
        }
        if self.finishing.load(Ordering::Acquire) {
            self.advance_finish()?;
        } else {
            self.flush_pending(false)?;
        }
        Ok(SinkStep::new(self.next_work(generation), 0))
    }
    fn finish(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<SinkStep> {
        self.finishing.store(true, Ordering::Release);
        SinkKernel::poll(self, ctx)
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
fn split_oversized_chunks_core<E>(
    chunks: Vec<Chunk>,
    target_bytes: usize,
    mut initial: impl FnMut(usize) -> Result<(), E>,
    mut before_push: impl FnMut(&Vec<Chunk>) -> Result<(), E>,
    mut measure: impl FnMut(&Chunk) -> Result<(usize, usize), E>,
    mut slice: impl FnMut(&Chunk, usize, usize) -> Result<Chunk, E>,
) -> Result<Vec<Chunk>, E> {
    let target_bytes = target_bytes.max(1);
    initial(chunks.len())?;
    let mut out = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        let (bytes, rows) = measure(&chunk)?;
        if rows <= 1 || bytes <= target_bytes {
            before_push(&out)?;
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
            let value = slice(&chunk, offset, len)?;
            before_push(&out)?;
            out.push(value);
            offset += len;
        }
    }
    Ok(out)
}

fn project_chunk_by_slot_ids_with_creation(
    chunk: &Chunk,
    slot_ids: &[SlotId],
    factory: &dyn crate::exec::pipeline::operator::InputConstructionFactory,
) -> ExecutionResult<Chunk> {
    use crate::exec::chunk::{ArrayNormalizationStep, ChunkSchema, ChunkSlotSchema};
    use crate::exec::pipeline::operator::ConstructionFailure;
    use novarocks_type_contract::owned_resources::layout::{LayoutResourceError, arc_layout};
    if slot_ids.is_empty() {
        return Ok(chunk.clone());
    }
    let owner = factory.begin_input_construction(chunk)?;
    let storage = chunk.source_storage_lifetime_observed(owner.storage_lifetime(), |layout| {
        owner.request(layout)
    })?;
    owner.request(
        std::alloc::Layout::array::<ChunkSlotSchema>(slot_ids.len())
            .map_err(|_| LayoutResourceError::Arithmetic)
            .map_err(ConstructionFailure::from)?,
    )?;
    let mut slots = Vec::with_capacity(slot_ids.len());
    owner.request(
        std::alloc::Layout::array::<ArrayRef>(slot_ids.len())
            .map_err(|_| LayoutResourceError::Arithmetic)
            .map_err(ConstructionFailure::from)?,
    )?;
    let mut columns = Vec::with_capacity(slot_ids.len());
    for slot_id in slot_ids {
        owner.observe(
            0,
            &[],
            ArrayNormalizationStep::InspectConstructionWork { units: 1 },
        )?;
        let index = chunk.chunk_schema().index_of(*slot_id).ok_or_else(|| {
            stream_contract(format_args!(
                "output_columns slot id {slot_id} missing from chunk schema"
            ))
        })?;
        slots.push(chunk.chunk_schema().slots()[index].clone());
        columns.push(chunk.columns()[index].clone());
    }
    let schema = ChunkSchema::try_new_typed_observed::<ConstructionFailure>(
        slots,
        storage.clone(),
        |layout| owner.request(layout),
        |units| {
            owner.observe(
                0,
                &[],
                ArrayNormalizationStep::InspectConstructionWork { units },
            )
        },
    )?;
    owner.request(
        arc_layout(std::alloc::Layout::new::<ChunkSchema>()).map_err(ConstructionFailure::from)?,
    )?;
    Ok(Chunk::try_new_typed_with_columns_owned_observed::<
        ConstructionFailure,
        _,
    >(
        Arc::new(schema),
        columns,
        chunk.len(),
        storage,
        |step| owner.observe(0, &[], step),
    )?)
}

#[cfg(test)]
fn split_oversized_chunks(chunks: Vec<Chunk>, target_bytes: usize) -> Vec<Chunk> {
    // Genuine legacy algorithm reference, not a production creator fallback.
    split_oversized_chunks_core::<std::convert::Infallible>(
        chunks,
        target_bytes,
        |_| Ok(()),
        |_| Ok(()),
        |chunk| Ok((chunk.record_batch().get_array_memory_size(), chunk.len())),
        |chunk, offset, length| Ok(chunk.slice(offset, length)),
    )
    .unwrap()
}
struct SplitChunkHeaders {
    chunks: Vec<Chunk>,
    _storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
fn split_oversized_chunks_with_creation(
    chunks: Vec<Chunk>,
    target_bytes: usize,
    factory: &dyn crate::exec::pipeline::operator::InputConstructionFactory,
) -> ExecutionResult<SplitChunkHeaders> {
    use crate::exec::pipeline::operator::ConstructionFailure;
    use novarocks_type_contract::owned_resources::layout::LayoutResourceError;
    use novarocks_type_contract::{ControlResourceError, ValueTypeStorageLifetime};
    let Some(first) = chunks.first() else {
        return Ok(SplitChunkHeaders {
            chunks,
            _storage: ValueTypeStorageLifetime::pure(),
        });
    };
    let headers = factory.begin_input_construction(first)?;
    let result =
        split_oversized_chunks_core::<ExecutionFailure>(
            chunks,
            target_bytes,
            |count| {
                headers.request(std::alloc::Layout::array::<Chunk>(count).map_err(|_| {
                    ConstructionFailure::Resource(LayoutResourceError::Arithmetic)
                })?)?;
                Ok(())
            },
            |out| {
                let growth = novarocks_type_contract::owned_resources::vec::push_growth(out)
                    .map_err(|error| -> ExecutionFailure {
                        match error {
                            ControlResourceError::Control(cause) => {
                                ConstructionFailure::Control(cause).into()
                            }
                            ControlResourceError::SourceModel(message) => {
                                ConstructionFailure::InvalidSource(message.into()).into()
                            }
                        }
                    })?;
                if let Some(layout) = growth.requested_backing {
                    headers.request(layout)?;
                }
                Ok(())
            },
            |chunk| {
                factory.observe_input_work(0)?;
                // Existing estimate selects the original row split; it is never an
                // allocation invoice, payload grant or retained-byte measurement.
                let shape = (chunk.record_batch().get_array_memory_size(), chunk.len());
                factory.observe_input_work(0)?;
                Ok(shape)
            },
            |chunk, offset, length| {
                let owner = factory.begin_input_construction(chunk)?;
                Ok(chunk.slice_typed_observed::<ConstructionFailure, _>(
                    offset,
                    length,
                    owner.storage_lifetime(),
                    |step| owner.observe(0, &[], step),
                )?)
            },
        )?;
    Ok(SplitChunkHeaders {
        chunks: result,
        _storage: headers.storage_lifetime(),
    })
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod partition_extent_failure_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection, RepresentationBoundary};
    #[test]
    fn selection_address_extent_is_representation_not_sql_numeric_overflow() {
        assert!(check_partition_row_extent(0).is_ok());
        assert!(check_partition_row_extent(1).is_ok());
        if usize::BITS > 32 {
            let last_legal_rows = (u64::from(u32::MAX) + 1) as usize;
            assert!(check_partition_row_extent(last_legal_rows).is_ok());
            let failure = check_partition_row_extent(last_legal_rows + 1).unwrap_err();
            let fact = failure.failure_fact();
            assert_eq!(fact.class(), FailureClass::Representation);
            assert_eq!(fact.reason().code(), 364);
            let extent = fact.representation().unwrap();
            assert_eq!(extent.boundary, RepresentationBoundary::SingleOutput);
            assert_eq!(extent.required, Some(u64::from(u32::MAX) + 1));
            assert_eq!(extent.extent_limit, Some(u64::from(u32::MAX)));
        }
    }
}

#[cfg(test)]
mod created_partition_regressions {
    use super::*;
    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::{Field, Schema};
    use novarocks_types::SlotId;

    fn input() -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![7, 2, 9, 2, 11])) as ArrayRef],
        )
        .unwrap();
        crate::exec::chunk::typed_fixture::chunk(
            batch,
            &[SlotId::new(1)],
            &[novarocks_type_contract::FunctionValueType::new(
                DataType::Int32,
                false,
            )],
        )
    }
    fn factory() -> Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory> {
        crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            crate::runtime::mem_tracker::MemTracker::new_root("partition regression construction"),
            Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
        )
        .unwrap()
    }
    fn values(chunk: &Chunk) -> Vec<i32> {
        if chunk.is_empty() {
            return Vec::new();
        }
        chunk.columns()[0]
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec()
    }
    #[test]
    fn real_partition_creator_preserves_hash_placement_order_and_binding() {
        let source = input();
        let factory = factory();
        let state = RuntimeState::default();
        let control = RuntimeKernelControl::new(state.error_state());
        for crc in [false, true] {
            let keys = vec![source.columns()[0].clone()];
            let expected = partition_chunk_by_hash_arrays(&source, &keys, 3, crc).unwrap();
            let actual = partition_chunk_by_hash_arrays_created(
                &source,
                &keys,
                3,
                crc,
                factory.as_ref(),
                &|| control.checkpoint_execution(),
            )
            .unwrap();
            for (expected, actual) in expected.iter().zip(actual.iter()) {
                assert_eq!(values(actual), values(expected));
                if !actual.is_empty() {
                    assert_eq!(
                        actual.chunk_schema().slots()[0].typed_binding(),
                        source.chunk_schema().slots()[0].typed_binding()
                    );
                }
            }
        }
    }
    #[test]
    fn actual_partition_creator_preserves_original_stopped_failure() {
        let source = input();
        let factory = factory();
        let state = RuntimeState::default();
        let original = crate::runtime::task::ExecutionFailure::from(
            novarocks_functions::KernelFailure::Operational(
                novarocks_functions::KernelDiagnostic::new("partition fragment failed"),
            ),
        );
        state.error_state().set_error(original.clone());
        let control = RuntimeKernelControl::new(state.error_state());
        let error = take_partition_rows_created(&source, &[1, 3], factory.as_ref(), &|| {
            control.checkpoint_execution()
        })
        .unwrap_err();
        assert_eq!(error, original);
    }
}
