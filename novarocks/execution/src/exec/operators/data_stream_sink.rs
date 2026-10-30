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
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};

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
            crate::runtime::fragment::ExecutionReason::ExchangeStreamContract.reason(),
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
        use crate::runtime::fragment::ExecutionReason;
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
            crate::runtime::fragment::ExecutionReason::ExchangeDictionaryKey.reason(),
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
            crate::runtime::fragment::ExecutionReason::ExchangePartitionRowExtent.reason(),
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
use crate::runtime::endpoint::FragmentDestination;
use crate::runtime::exchange;
use crate::runtime::fragment::io::exchange_queue::{ExchangeSendTask, ExchangeSendTracker};
use crate::runtime::fragment::io::{
    EdgeSendGate, EdgeSendState, ExchangeDestinationKey, ExchangeEdgeGates, ExchangeFrame,
    ExchangeFrameTransmitter,
};
use crate::runtime::mem_tracker::{MemTracker, TrackedBytes};
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
    input::{PreparedInput, RawInput},
    operator::{ExitObservation, FinishWatch, InputEncodingPolicy, Operator},
    shape::{ControlStep, SinkKernel, SinkStep, Transform},
    step::{AdmissionNext, NextStep, WaitFact, WaitReason},
    step_context::StepContext,
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
    use super::{
        Chunk, StreamDictionaryKeyFailure, check_partition_row_extent,
        is_low_cardinality_exchange_dictionary, partition_arrow, stream_contract,
    };
    use crate::exec::hash_table::hash::{canonical_f32_bits, canonical_f64_bits};
    use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
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
    ) -> crate::runtime::fragment::ExecutionResult<Chunk> {
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
        let output = take_partition_rows_with::<crate::runtime::fragment::ExecutionFailure>(
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
    ctx: &StepContext<'_>,
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
    ctx: &mut StepContext<'_>,
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
    fragment_instance_id: UniqueId,
    sender_id: Option<i32>,
    transmitter: Arc<dyn ExchangeFrameTransmitter>,
    plan_node_id: i32,
    finish_state: Arc<DataStreamSinkFinishState>,
    shared_order: Arc<crate::runtime::fragment::io::exchange_queue::ExchangeSendOrder>,
    edge_gates: Option<Arc<ExchangeEdgeGates>>,
    compiled_partition: Option<CompiledPartitionKeys>,
}

impl DataStreamSinkFactory {
    pub fn new(
        input: DataStreamSinkFactoryInput,
        fragment_instance_id: UniqueId,
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
            fragment_instance_id,
            sender_id,
            transmitter,
            plan_node_id,
            finish_state: Arc::new(DataStreamSinkFinishState::default()),
            shared_order: crate::runtime::fragment::io::exchange_queue::ExchangeSendOrder::new(),
            edge_gates: None,
            compiled_partition: None,
        }
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
    /// frontend opens it. A sink left ungated keeps its ungated behavior,
    /// which is what the fragment paths that have no frozen edge topology
    /// still rely on.
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

        let init_error = self.init_error.clone();

        let send_observable = Arc::new(Observable::new());

        Box::new(Transform::hydrating(DataStreamSinkOperator {
            name: self.name.clone(),
            input: self.input.clone(),
            fragment_instance_id: self.fragment_instance_id,
            transmitter: Arc::clone(&self.transmitter),
            init_error,
            driver_id,
            sender_id,
            be_number,
            shared_order: Arc::clone(&self.shared_order),
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
            send_tracker: ExchangeSendTracker::new(),
            send_observable,
            construction_factory: None,
            prepared: false,
            exiting: false,
            compiled_control: None,
            error_state: None,
            finish_state: Arc::clone(&self.finish_state),
            profile_initialized: false,
            profiles: None,
            pending_chunks_mem_tracker: None,
            pending_payload_mem_tracker: None,
            send_queue_mem_tracker: None,
            exchange_queue: None,
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
    /// The destination's edge was closed by a normal downstream cancellation,
    /// so the payload is dropped rather than parked or sent.
    Discarded,
}

struct DataStreamSinkOperator {
    name: String,
    input: DataStreamSinkFactoryInput,
    fragment_instance_id: UniqueId,
    transmitter: Arc<dyn ExchangeFrameTransmitter>,
    init_error: Option<ExecutionFailure>,
    driver_id: i32,
    sender_id: i32,
    be_number: i32,
    shared_order: Arc<crate::runtime::fragment::io::exchange_queue::ExchangeSendOrder>,
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
    send_tracker: Arc<ExchangeSendTracker>,
    send_observable: Arc<Observable>,
    error_state: Option<Arc<RuntimeErrorState>>,
    finish_state: Arc<DataStreamSinkFinishState>,
    profile_initialized: bool,
    profiles: Option<crate::runtime::profile::OperatorProfiles>,
    pending_chunks_mem_tracker: Option<Arc<MemTracker>>,
    pending_payload_mem_tracker: Option<Arc<MemTracker>>,
    send_queue_mem_tracker: Option<Arc<MemTracker>>,
    exchange_queue: Option<Arc<crate::runtime::fragment::io::exchange_queue::ExchangeSendQueue>>,
    edge_gates: Option<Arc<ExchangeEdgeGates>>,
    compiled_partition: Option<CompiledPartitionState>,
    construction_factory:
        Option<Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>>,
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

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.error_state = Some(state.error_state());
        let mut control = RuntimeKernelControl::new(state.error_state());
        control.bind_runtime_memory(state);
        if let Some(tracker) = &self.pending_chunks_mem_tracker {
            control.bind_mem_tracker(tracker.clone());
        }
        if let Some(factory) = &self.construction_factory {
            control.bind_construction_factory(factory.clone());
        }
        self.compiled_control = Some(control);
        #[cfg(novarocks_dev_controls)]
        if let Some(observer) = state.exchange_test_observer() {
            self.send_tracker.install_test_observer(observer.clone())?;
        }
        self.be_number = state.backend_num().unwrap_or(0);
        self.max_transmit_batched_bytes = state
            .execution_runtime()
            .map(|runtime| runtime.config().exchange_max_transmit_batched_bytes)
            .unwrap_or(1)
            .max(1);
        let Some(runtime) = state.execution_runtime() else {
            #[cfg(test)]
            return Ok(());
            #[cfg(not(test))]
            return Err(
                crate::runtime::runtime_state::RuntimeBindingFailure::MissingExecutionRuntime
                    .into(),
            );
        };
        let queue = runtime.exchange_send_queue();
        queue.register_send_observer(&self.send_observable);
        self.exchange_queue = Some(queue);
        Ok(())
    }

    fn prepare(&mut self) -> ExecutionResult<()> {
        if self.prepared {
            return Ok(());
        }
        self.construction_factory
            .as_ref()
            .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?;
        self.prepared = true;
        // Align with StarRocks: count actual sink drivers prepared, not planned DOP.
        self.finish_state.register_driver();
        // The edge opening is the only event that can unpark this driver once
        // its payload is parked behind a closed edge, and the executor
        // re-checks an output-blocked driver only when an observable that
        // driver registered fires. Registering here, rather than at runtime
        // binding, is deliberate: `bind_runtime_state` returns early without
        // an `ExecutionRuntime`, and the gate wake-up must not depend on
        // whether a send queue was bound.
        if let Some(gates) = self.edge_gates.as_ref() {
            gates.register_open_waiter(&self.send_observable);
        }
        tracing::debug!(
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

    #[cfg(test)]
    #[cfg(test)]
    fn pending_finish(&self) -> Option<crate::exec::pipeline::operator::FinishWatch> {
        (!self.send_tracker.is_idle()).then(|| {
            crate::exec::pipeline::operator::FinishWatch::Notify(Arc::clone(&self.send_observable))
        })
    }

    fn begin_exit(&mut self) -> ExecutionResult<()> {
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
        if (self.exiting || self.aborting.load(Ordering::Acquire)) && !self.send_tracker.is_idle() {
            ExitObservation::pending(FinishWatch::Notify(self.send_observable.clone()))
        } else {
            ExitObservation::exited()
        }
    }
    fn cancel(&mut self) {
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
        if let Some(queue) = self.exchange_queue.as_ref() {
            queue.cancel_tracker(&self.send_tracker);
        }
        self.send_observable.defer_notify().arm();
    }
}

impl DataStreamSinkOperator {
    fn exchange_queue(
        &self,
    ) -> &Arc<crate::runtime::fragment::io::exchange_queue::ExchangeSendQueue> {
        self.exchange_queue
            .as_ref()
            .expect("exchange sink must be bound to an ExecutionRuntime before use")
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
        dest: &FragmentDestination,
    ) -> ExecutionResult<Option<Arc<EdgeSendGate>>> {
        let Some(gates) = self.edge_gates.as_ref() else {
            return Ok(None);
        };
        let key = ExchangeDestinationKey::new(*dest.finst_id(), self.input.dest_node_id);
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
        let max_inflight = self.exchange_queue().max_inflight_bytes();
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
            if payload.payload_bytes > max_inflight {
                continue;
            }
            if !self.exchange_queue().can_reserve(payload.payload_bytes) {
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
        use tracing::debug;
        let inflight_bytes = self.exchange_queue().inflight_bytes();
        let max_inflight_bytes = self.exchange_queue().max_inflight_bytes();
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

    fn current_error(&self) -> Option<ExecutionFailure> {
        if let Some(error) = self.error_state.as_ref().and_then(|state| state.error()) {
            return Some(error);
        }
        if let Some(error) = self.send_tracker.failure() {
            return Some(error);
        }
        if self.input.delivery
            == novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain
            && !self.aborting.load(Ordering::Acquire)
            && !self.end_of_stream_sent.load(Ordering::Acquire)
            && self
                .destinations()
                .iter()
                .enumerate()
                .any(|(i, d)| !Self::is_pseudo_destination(d) && self.destination_withdrawn(i))
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
            if self.send_tracker.is_idle() {
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
            && self.send_tracker.is_idle()
        {
            if !self.finished.swap(true, Ordering::AcqRel) {
                // The one line that says this sink stopped owing output, and
                // on whose strength the driver terminates. Absent it, a sink
                // that finished with its seal unsent is indistinguishable
                // from one that never got that far.
                tracing::debug!(
                    "DataStreamSink finished: finst={} driver_id={} dest_node_id={} sender_id={} end_of_stream_sent={} last_driver={}",
                    format_uuid(
                        self.fragment_instance_id.high(),
                        self.fragment_instance_id.low()
                    ),
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

    /// Evaluate applicability once over the original payload. Only its TRUE
    /// rows demand key roots; selected INSERT and no-match rows keep their
    /// original payload position without reading a target key.
    fn compiled_partition_keys(
        &mut self,
        ctx: &StepContext<'_>,
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
        ctx: &mut StepContext<'_>,
        chunk: &Chunk,
        compiled_keys: Option<&CompiledPartitionBatch>,
    ) -> ExecutionResult<Vec<Vec<Chunk>>> {
        // Get destinations first to avoid borrowing self
        let dests: Vec<FragmentDestination> = self.destinations().to_vec();
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
        dest: &FragmentDestination,
        mut pending: PendingPayload,
        allow_overflow: bool,
    ) -> ExecutionResult<PayloadEnqueue> {
        if dest.source_finst_id() != self.fragment_instance_id {
            return Err(stream_contract(format_args!(
                "exchange destination source fragment instance {}:{} does not match sender {}:{}",
                dest.source_finst_id().high(),
                dest.source_finst_id().low(),
                self.fragment_instance_id.high(),
                self.fragment_instance_id.low(),
            )));
        }
        let stream = match (
            self.error_state.as_ref().and_then(|s| s.task_identity()),
            dest.task_identity(),
        ) {
            (Some(source), Some(target)) => Some(
                novarocks_execution_contract::ExchangeStreamIdentity::try_new(
                    novarocks_execution_contract::ExchangeSource::new(
                        source,
                        dest.source_finst_id(),
                        dest.sender_ordinal(),
                    ),
                    target,
                    *dest.finst_id(),
                    novarocks_execution_contract::FragmentNodeId::new(self.input.dest_node_id),
                )
                .map_err(stream_contract)?,
            ),
            (None, None) => None,
            _ => {
                return Err(stream_contract(
                    "exchange Task source and destination identities must both be bound",
                ));
            }
        };
        let edge_gate = self.destination_gate(dest)?;
        match edge_gate.as_ref().map(|gate| gate.state()) {
            None | Some(EdgeSendState::Open) => {}
            // A closed edge sends nothing and reserves nothing. The payload
            // keeps the sink's existing single per-destination slot, which is
            // what makes waiting for permission bounded backpressure rather
            // than a second buffer.
            Some(EdgeSendState::AwaitingPermission) => {
                return Ok(PayloadEnqueue::NoCapacity(pending));
            }
            // The destination left normally, so its frames are dropped here
            // rather than parked or sent.
            Some(EdgeSendState::NormallyCanceled) => {
                if self.input.delivery
                    == novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain
                {
                    return Err(stream_contract(
                        "required exchange destination withdrew before payload admission",
                    ));
                }
                return Ok(PayloadEnqueue::Discarded);
            }
        }

        let allow_overflow =
            allow_overflow || pending.payload_bytes > self.exchange_queue().max_inflight_bytes();
        let reserve_bytes = pending.payload_bytes.max(1);

        let addr = dest.endpoint();
        let dest_finst_id = UniqueId::new(dest.finst_id().high(), dest.finst_id().low());
        let error_state = self
            .error_state
            .as_ref()
            .ok_or_else(|| stream_contract("missing runtime error state"))?;
        if !allow_overflow
            && !self.exchange_queue().reserve_bytes_for(
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
            dest.clone(),
            stream,
            pending,
            Arc::clone(error_state),
            edge_gate,
        );
        if allow_overflow {
            self.exchange_queue().try_submit(task, true)?;
            return Ok(PayloadEnqueue::Enqueued);
        }
        self.exchange_queue().submit_reserved(task, reserve_bytes)?;
        Ok(PayloadEnqueue::Enqueued)
    }

    fn build_exchange_send_task(
        &self,
        destination: FragmentDestination,
        stream: Option<novarocks_execution_contract::ExchangeStreamIdentity>,
        pending: PendingPayload,
        error_state: Arc<RuntimeErrorState>,
        edge_gate: Option<Arc<EdgeSendGate>>,
    ) -> ExchangeSendTask {
        #[cfg(test)]
        record_payload_identity_for_test(self.fragment_instance_id, pending.be_number, pending.eos);
        ExchangeSendTask {
            order: self.shared_order.clone(),
            frame: ExchangeFrame {
                stream,
                delivery: self.input.delivery,
                destination: destination.endpoint().clone(),
                destination_fragment_instance_id: *destination.finst_id(),
                destination_task_identity: destination.task_identity(),
                sender_fragment_instance_id: destination.source_finst_id(),
                sender_ordinal: destination.sender_ordinal(),
                sender_count: destination.sender_count(),
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
            edge_gate,
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
    ) -> ExecutionResult<PayloadEnqueue> {
        use tracing::debug;

        self.init_profile_if_needed();

        let dest_finst_id = UniqueId::new(dest.finst_id().high(), dest.finst_id().low());

        let row_count: usize = chunks.iter().map(|c| c.len()).sum();
        // The queue assigns the real sequence only when this payload is admitted.
        let sequence = 0;
        debug!(
            "DataStreamSink::transmit_partition: dest_finst={} node_id={} sender_id={} chunks={} rows={} eos={} pending_sequence={}",
            dest_finst_id,
            self.input.dest_node_id,
            self.sender_id,
            chunks.len(),
            row_count,
            eos,
            sequence
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
        // Delivery is asynchronous, so every data payload must be self-describing.
        let include_slot_ids = Self::should_include_wire_meta(chunks.is_empty());
        let encode_start = std::time::Instant::now();
        let error_state = self
            .error_state
            .as_ref()
            .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?;
        let control = RuntimeKernelControl::new(error_state.clone());
        let payload = exchange::encode_chunks_with_creation(
            chunks,
            include_slot_ids,
            factory.as_ref(),
            &control,
        )?;
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
                    "DataStreamSink::transmit_partition enqueued: dest_finst={} node_id={} eos={} pending_sequence={} bytes={}",
                    dest_finst_id, self.input.dest_node_id, eos, sequence, payload_bytes
                );
                Ok(PayloadEnqueue::Enqueued)
            }
            PayloadEnqueue::Discarded => {
                debug!(
                    "DataStreamSink::transmit_partition discarded: dest_finst={} node_id={} eos={} pending_sequence={} bytes={} reason=destination_cancelled_normally",
                    dest_finst_id, self.input.dest_node_id, eos, sequence, payload_bytes
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
            crate::exec::pipeline::step_context::TurnWork::new(std::num::NonZeroU64::MIN, 64);
        let mut value_conversions =
            novarocks_value_arrays::decode::ValueConversionContext::default();
        let mut ctx = StepContext::new(&control, &mut work, &mut value_conversions);
        self.buffer_chunk_with_keys(&mut ctx, chunk, None)
    }

    fn buffer_chunk_with_keys(
        &mut self,
        ctx: &mut StepContext<'_>,
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

    fn drain_pending_batch(&mut self, dest_idx: usize) -> ExecutionResult<(Vec<Chunk>, usize)> {
        let max_bytes = self.max_transmit_batched_bytes.max(1);
        let pending = self
            .pending_per_dest
            .get_mut(dest_idx)
            .ok_or_else(|| stream_contract("pending buffer index out of range"))?;
        if pending.is_empty() {
            return Ok((Vec::new(), 0));
        }

        let mut batch = Vec::new();
        let mut batch_bytes = 0usize;
        let mut first_schema: Option<crate::exec::chunk::ChunkSchemaRef> = None;
        while let Some(front) = pending.front() {
            let chunk_bytes = front.record_batch().get_array_memory_size();
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
                .ok_or_else(|| stream_contract("pending buffer empty unexpectedly"))?;
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

    fn flush_pending(&mut self, force: bool, allow_overflow: bool) -> ExecutionResult<()> {
        self.ensure_pending_buffers_initialized();
        let dests: Vec<FragmentDestination> = self.destinations().to_vec();
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
                match self.try_enqueue_payload(dest, payload, allow_overflow)? {
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
                let (chunks, _batch_bytes) = self.drain_pending_batch(i)?;
                if chunks.is_empty() {
                    break;
                }
                match self.transmit_partition(i, dest, &chunks, false, allow_overflow)? {
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
        let dests: Vec<FragmentDestination> = self.destinations().to_vec();
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
                        dest.finst_id()
                    )));
                }
                None | Some(EdgeSendState::Open) => {}
            }
            match self.transmit_partition(i, dest, &[], true, true)? {
                PayloadEnqueue::Enqueued | PayloadEnqueue::Discarded => sealed += 1,
                PayloadEnqueue::NoCapacity(_) => {
                    return Err(stream_contract("exchange send EOS unexpectedly blocked"));
                }
            }
        }
        // Latched only now that every destination this sink owes has its
        // marker: the receiver's sender count closes on this and on nothing
        // else, so the latch has to describe the send that happened rather
        // than the intent to send.
        self.end_of_stream_sent.store(true, Ordering::SeqCst);
        tracing::debug!(
            "DataStreamSink end-of-stream sent: finst={} driver_id={} dest_node_id={} sender_id={} sealed={} skipped={}",
            format_uuid(
                self.fragment_instance_id.high(),
                self.fragment_instance_id.low()
            ),
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
        if pending_reserve_bytes > 0 && !self.exchange_queue().can_reserve(pending_reserve_bytes) {
            self.log_need_input_blocked("reserve_bytes", pending_reserve_bytes);
            return false;
        }
        true
    }
    fn advance_finish(&mut self) -> ExecutionResult<()> {
        use tracing::debug;

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
        // The gate state per outbound edge, and the state of the gate each
        // destination of THIS sink resolves to. A sink that waits forever is
        // waiting on one of these, and without both halves it cannot be told
        // apart from a sink whose edge opened but whose data never drained.
        debug!(
            "DataStreamSink finishing gates: finst={} driver_id={} dest_node_id={} edges={:?} destination_gates={:?} awaits_permission={} has_pending_data={}",
            format_uuid(
                self.fragment_instance_id.high(),
                self.fragment_instance_id.low()
            ),
            self.driver_id,
            self.input.dest_node_id,
            self.edge_gates
                .as_ref()
                .map(|gates| gates.edges().collect::<Vec<_>>()),
            self.destinations()
                .iter()
                .enumerate()
                .map(|(idx, dest)| (
                    dest.finst_id().low(),
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
        // The marker is output like any other frame, so it needs this
        // driver's edges open. The last-driver verdict is latched, so
        // returning here keeps it for the retry that `finishing_wait` asks
        // for. Pending data needs no second check: no driver reaches the
        // count while it still holds any.
        if self.awaits_edge_permission() {
            return Ok(());
        }
        self.send_eos()?;
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
    fn poll_control(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<ControlStep> {
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
        _: &mut StepContext<'_>,
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
        _ctx: &mut StepContext<'_>,
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
        self.flush_pending(false, false)?;
        if chunk.is_empty() {
            return Ok(SinkStep::new(self.next_work(generation), 0));
        }
        let compiled_keys = self.compiled_partition_keys(_ctx, &chunk)?;
        self.buffer_chunk_with_keys(_ctx, chunk, compiled_keys)?;
        self.flush_pending(false, false)?;
        Ok(SinkStep::new(self.next_work(generation), rows))
    }
    fn poll(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<SinkStep> {
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
            self.flush_pending(false, false)?;
        }
        Ok(SinkStep::new(self.next_work(generation), 0))
    }
    fn finish(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<SinkStep> {
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
mod tests {
    use super::*;
    use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
    use arrow::array::{
        Array, ArrayRef, BinaryArray, DictionaryArray, Int32Array, LargeStringArray, StringArray,
    };
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    use crate::runtime::endpoint::RuntimeEndpoint;

    fn make_test_operator() -> DataStreamSinkOperator {
        DataStreamSinkOperator {
            name: "DataStreamSink(test)".to_string(),
            input: DataStreamSinkFactoryInput::try_new(
                1,
                DataStreamPartitionType::Unpartitioned,
                novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
                Vec::new(),
                Vec::new(),
            )
            .expect("test sink input"),
            transmitter: crate::runtime::fragment::io::exchange::discard_exchange_transmitter(),
            fragment_instance_id: UniqueId::new(1, 2),
            init_error: None,
            driver_id: 0,
            sender_id: 11,
            be_number: 0,
            shared_order: crate::runtime::fragment::io::exchange_queue::ExchangeSendOrder::new(),
            random_next: 0,
            pending_per_dest: Vec::new(),
            pending_bytes_per_dest: Vec::new(),
            pending_payloads_per_dest: Vec::new(),
            max_transmit_batched_bytes: 1,
            finished: AtomicBool::new(false),
            aborting: AtomicBool::new(false),
            finishing: AtomicBool::new(false),
            finish_counted: AtomicBool::new(false),
            finish_completed_set: AtomicBool::new(false),
            end_of_stream_sent: AtomicBool::new(false),
            send_tracker: ExchangeSendTracker::new(),
            send_observable: Arc::new(Observable::new()),
            error_state: None,
            finish_state: Arc::new(DataStreamSinkFinishState::default()),
            profile_initialized: false,
            profiles: None,
            pending_chunks_mem_tracker: None,
            pending_payload_mem_tracker: None,
            send_queue_mem_tracker: None,
            exchange_queue: Some(Arc::new(
                crate::runtime::fragment::io::exchange_queue::ExchangeSendQueue::new(
                    1024,
                    Arc::new(crate::runtime::io::IoExecutor::new(1)),
                ),
            )),
            edge_gates: None,
            compiled_partition: None,
            construction_factory: None,
            prepared: false,
            exiting: false,
            compiled_control: None,
        }
    }

    fn prepare_test_operator(op: &mut DataStreamSinkOperator) {
        let error = op
            .error_state
            .get_or_insert_with(|| Arc::new(RuntimeErrorState::default()))
            .clone();
        let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            crate::runtime::mem_tracker::MemTracker::new_root("prepared exchange sink fixture"),
            error,
        ).expect("actual sink creator");
        let mut control = RuntimeKernelControl::new(op.error_state.as_ref().unwrap().clone());
        control.bind_runtime_memory(&RuntimeState::default());
        op.compiled_control = Some(control);
        op.set_construction_factory(factory);
        op.prepare().expect("actual sink preparation");
    }

    fn typed_int64_chunk(rows: usize) -> Chunk {
        use novarocks_type_contract::{
            TypedValueBinding, ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime,
        };
        let pure = ValueTypeStorageLifetime::pure();
        let value =
            ValueTypeRef::try_new(ValueKind::Scalar(DataType::Int64), false, pure.clone()).unwrap();
        let binding = TypedValueBinding::try_new(
            value,
            ValueLayout::try_new(&DataType::Int64, pure.clone()).unwrap(),
        )
        .unwrap();
        let slot = ChunkSlotSchema::try_new_typed(
            SlotId::new(1),
            Arc::new(Field::new("v", DataType::Int64, false)),
            binding,
            None,
            pure.clone(),
        )
        .unwrap();
        let schema = Arc::new(ChunkSchema::try_new_typed(vec![slot], pure).unwrap());
        // Explicit pure frozen source author; the runtime sender derives new
        // headers only through its genuinely installed creator above.
        Chunk::try_new_typed_with_columns_observed::<ExecutionFailure, _>(
            schema,
            vec![Arc::new(arrow::array::Int64Array::from_iter_values(
                0..rows as i64,
            ))],
            rows,
            |_| Ok(()),
        )
        .unwrap()
    }

    #[test]
    fn hash_placement_refuses_missing_destination_and_misaligned_key_rows() {
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        let chunk = typed_int64_chunk(2);
        for crc32 in [false, true] {
            let error = partition_chunk_by_hash_arrays(&chunk, &[], 0, crc32).unwrap_err();
            assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
            assert_eq!(error.failure_fact().reason().code(), 358);
            let key: arrow::array::ArrayRef = Arc::new(arrow::array::Int64Array::from(vec![1]));
            let error = partition_chunk_by_hash_arrays(&chunk, &[key], 2, crc32).unwrap_err();
            assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
            assert_eq!(error.failure_fact().reason().code(), 358);
            assert!(
                error
                    .detail()
                    .contains("key rows 1 differ from input rows 2")
            );
        }
    }

    struct BlockingEosTransmitter {
        entered: mpsc::Sender<()>,
        release: Mutex<mpsc::Receiver<()>>,
        calls: AtomicUsize,
    }

    impl ExchangeFrameTransmitter for BlockingEosTransmitter {
        fn transmit(
            &self,
            frame: ExchangeFrame,
        ) -> Result<(), crate::runtime::fragment::io::ExchangeTransmitRejection> {
            assert!(frame.eos, "empty sink sends only its EOS frame");
            self.calls.fetch_add(1, Ordering::AcqRel);
            self.entered.send(()).expect("test observes EOS send");
            let _ = self.release.lock().expect("release lock").recv();
            Ok(())
        }
    }

    fn blocked_eos_sink() -> (
        DataStreamSinkOperator,
        Arc<BlockingEosTransmitter>,
        mpsc::Receiver<()>,
        mpsc::Sender<()>,
    ) {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let transmitter = Arc::new(BlockingEosTransmitter {
            entered: entered_tx,
            release: Mutex::new(release_rx),
            calls: AtomicUsize::new(0),
        });
        let mut sink = make_test_operator();
        sink.transmitter = Arc::clone(&transmitter) as Arc<dyn ExchangeFrameTransmitter>;
        sink.input.destinations = vec![make_test_destination()];
        sink.error_state = Some(Arc::new(RuntimeErrorState::default()));
        prepare_test_operator(&mut sink);
        (sink, transmitter, entered_rx, release_tx)
    }

    #[test]
    fn normal_eos_does_not_finish_until_blocked_transmit_returns() {
        let (mut sink, transmitter, entered, release) = blocked_eos_sink();
        let data =
            SinkKernel::finish_for_test(&mut sink, &RuntimeState::default()).expect("enqueue EOS");
        assert!(
            matches!(data.next, NextStep::Finished),
            "the marker is enqueued, not yet transmitted"
        );
        entered
            .recv_timeout(Duration::from_secs(2))
            .expect("EOS transmit started");
        sink.begin_exit().expect("observe the real send owner");
        assert!(matches!(
            sink.observe_exit().progress,
            ExitProgress::Pending(_)
        ));
        release.send(()).expect("release real EOS transmit");
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match sink.observe_exit().progress {
                ExitProgress::Exited => break,
                ExitProgress::Pending(_) | ExitProgress::Unresolved(_) => {
                    assert!(Instant::now() < deadline, "EOS transmit exit deadline");
                    std::thread::yield_now();
                }
            }
        }
        assert_eq!(transmitter.calls.load(Ordering::Acquire), 1);
    }

    #[test]
    fn cancellation_revokes_eos_debt_but_waits_for_blocked_transmit() {
        let (mut sink, transmitter, entered, release) = blocked_eos_sink();
        let data =
            SinkKernel::finish_for_test(&mut sink, &RuntimeState::default()).expect("enqueue EOS");
        assert!(matches!(data.next, NextStep::Finished));
        entered
            .recv_timeout(Duration::from_secs(2))
            .expect("EOS transmit started");
        sink.cancel();
        sink.begin_exit()
            .expect("retain the inflight send after cancellation");
        assert!(matches!(
            sink.observe_exit().progress,
            ExitProgress::Pending(_)
        ));
        assert!(!sink.input_admissible());
        release.send(()).expect("release real EOS transmit");
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match sink.observe_exit().progress {
                ExitProgress::Exited => break,
                ExitProgress::Pending(_) | ExitProgress::Unresolved(_) => {
                    assert!(Instant::now() < deadline, "cancelled send exit deadline");
                    std::thread::yield_now();
                }
            }
        }
        assert_eq!(transmitter.calls.load(Ordering::Acquire), 1);
    }

    #[test]
    fn task_shutdown_waits_for_the_inflight_send_before_publishing_stopped() {
        use crate::exec::pipeline::driver::PipelineDriver;
        use crate::exec::pipeline::fragment_context::FragmentContext;
        use crate::exec::pipeline::global_driver_executor::{DriverTask, FragmentCompletion};

        let (mut sink, transmitter, entered, release) = blocked_eos_sink();
        SinkKernel::finish_for_test(&mut sink, &RuntimeState::default()).expect("enqueue EOS");
        entered
            .recv_timeout(Duration::from_secs(2))
            .expect("EOS transmit started");

        let state = Arc::new(RuntimeState::default());
        let driver = PipelineDriver::new(
            1,
            vec![Box::new(
                crate::exec::pipeline::shape::Transform::hydrating(sink),
            )],
            None,
            Vec::new(),
            Arc::clone(&state),
            None,
        );
        let completion = FragmentCompletion::new(1, state.error_state());
        let submitted_failure: crate::runtime::fragment::ExecutionFailure =
            (crate::exec::pipeline::driver_failure::DriverFailure::ExecutorShutdown).into();
        completion.fail(submitted_failure.clone());
        assert!(
            completion
                .conclusion()
                .unwrap()
                .unwrap_err()
                .same_occurrence(&submitted_failure)
        );
        let context = Arc::new(FragmentContext::new(None, state, None, None, None, None));
        let task = DriverTask::new(
            driver,
            Arc::clone(&completion),
            context,
            Duration::from_millis(10),
        );
        let mut task = task
            .finish_due_to_abort()
            .expect("blocked transmit keeps task pending");
        assert!(completion.stopped_fact().is_none());

        release.send(()).expect("release real EOS transmit");
        let deadline = Instant::now() + Duration::from_secs(2);
        while task.has_pending_finish() && Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert!(!task.has_pending_finish());
        assert!(task.finish_due_to_abort().is_none());
        assert!(completion.stopped_fact().is_some());
        assert_eq!(transmitter.calls.load(Ordering::Acquire), 1);
    }

    fn make_test_destination() -> FragmentDestination {
        FragmentDestination::new(
            UniqueId::new(9, 9),
            RuntimeEndpoint::new("127.0.0.1", 9030).expect("endpoint"),
            UniqueId::new(1, 2),
            0,
            1,
        )
        .expect("destination")
    }

    fn destination_with_finst(low: i64) -> FragmentDestination {
        FragmentDestination::new(
            UniqueId::new(9, low),
            RuntimeEndpoint::new("127.0.0.1", 9030).expect("endpoint"),
            UniqueId::new(1, 2),
            0,
            1,
        )
        .expect("destination")
    }

    fn edge_id(value: u32) -> ExchangeEdgeId {
        ExchangeEdgeId::new(value).expect("nonzero edge")
    }

    /// Gates the sink's destinations one edge per destination, all closed, so
    /// each edge's decision is observable in isolation.
    fn gate_destinations(op: &mut DataStreamSinkOperator, dests: Vec<FragmentDestination>) {
        let gates = ExchangeEdgeGates::try_new(dests.iter().enumerate().map(|(idx, dest)| {
            (
                edge_id(idx as u32 + 1),
                vec![ExchangeDestinationKey::new(
                    *dest.finst_id(),
                    op.input.dest_node_id,
                )],
            )
        }))
        .expect("legal gate set");
        op.input.destinations = dests;
        op.edge_gates = Some(gates);
    }

    /// One version opens one exact edge set, so every edge a test needs open
    /// is named in a single request.
    fn open_edges(op: &DataStreamSinkOperator, edges: &[ExchangeEdgeId]) {
        op.edge_gates
            .as_ref()
            .expect("gated sink")
            .open(crate::task_execution::domain::EdgeOpenVersion::FIRST, edges)
            .expect("open edges");
    }

    fn cancel_edge(op: &DataStreamSinkOperator, edge: ExchangeEdgeId) {
        assert!(
            op.edge_gates
                .as_ref()
                .expect("gated sink")
                .gate(edge)
                .expect("gated edge")
                .close_for_normal_cancellation()
        );
    }

    fn test_payload(bytes: usize) -> PendingPayload {
        PendingPayload {
            be_number: 0,
            payload: vec![7; bytes],
            payload_bytes: bytes,
            encode_ns: 0,
            sequence: 0,
            eos: false,
            accounting: None,
        }
    }

    fn make_test_exchange_send_task() -> ExchangeSendTask {
        let mut op = make_test_operator();
        op.fragment_instance_id = UniqueId::new(81, 82);
        op.build_exchange_send_task(
            FragmentDestination::new(
                UniqueId::new(91, 92),
                RuntimeEndpoint::new("127.0.0.1", 9030).expect("endpoint"),
                UniqueId::new(81, 82),
                0,
                1,
            )
            .expect("destination"),
            None,
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
            None,
        )
    }

    fn zero_column_chunk_with_rows(row_count: usize) -> Chunk {
        let schema = Arc::new(
            ChunkSchema::try_new_typed(
                Vec::new(),
                novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            )
            .unwrap(),
        );
        Chunk::try_new_typed_with_columns_observed::<crate::runtime::fragment::ExecutionFailure, _>(
            schema,
            Vec::new(),
            row_count,
            |_| Ok(()),
        )
        .expect("explicit count-only source")
    }

    #[test]
    fn data_stream_sink_accepts_low_cardinality_dictionary_carriers() {
        let op = make_test_operator();
        let utf8_dict = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let large_utf8_dict =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::LargeUtf8));

        assert!(op.retains_encoded_column(SlotId::new(91), &utf8_dict));
        assert!(op.retains_encoded_column(SlotId::new(91), &large_utf8_dict));
    }

    #[test]
    fn data_stream_sink_rejects_non_string_or_non_int32_dictionaries() {
        let op = make_test_operator();
        let wrong_key = DataType::Dictionary(Box::new(DataType::Int64), Box::new(DataType::Utf8));
        let wrong_value =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int32));

        assert!(!op.retains_encoded_column(SlotId::new(91), &wrong_key));
        assert!(!op.retains_encoded_column(SlotId::new(91), &wrong_value));
        assert!(!op.retains_encoded_column(SlotId::new(91), &DataType::Utf8));
    }

    #[test]
    fn data_stream_sink_accepts_dictionary_for_hash_partition_after_hash_support() {
        let mut op = make_test_operator();
        op.input.output_partition_type = DataStreamPartitionType::HashPartitioned;
        let utf8_dict = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));

        assert!(op.retains_encoded_column(SlotId::new(91), &utf8_dict));
    }

    #[test]
    fn bind_runtime_state_uses_backend_num_as_be_number() {
        let mut op = make_test_operator();
        let state = RuntimeState::for_local_execution(None, None, None, None, Some(7), None, None);

        OperatorLifecycle::bind_runtime_state(&mut op, &state).expect("bind runtime state");

        assert_eq!(op.sender_id, 11);
        assert_eq!(op.be_number, 7);
    }

    #[test]
    fn zero_byte_pending_chunks_are_tracked_for_force_flush() {
        let mut op = make_test_operator();
        op.input.destinations = vec![make_test_destination()];

        prepare_test_operator(&mut op);
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
        assert_eq!(task.frame.sender_ordinal, 0);
        assert_eq!(task.frame.sender_count, 1);
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
    fn a_closed_edge_enqueues_no_frame_and_reserves_no_bytes() {
        let mut op = make_test_operator();
        let dest = make_test_destination();
        gate_destinations(&mut op, vec![dest.clone()]);
        op.error_state = Some(Arc::new(RuntimeErrorState::default()));

        assert!(matches!(
            op.try_enqueue_payload(&dest, test_payload(4), false),
            Ok(PayloadEnqueue::NoCapacity(_))
        ));
        assert!(
            op.send_tracker.is_idle(),
            "a closed edge must hand nothing to the send queue"
        );
        assert_eq!(
            op.exchange_queue().inflight_bytes(),
            0,
            "a closed edge must not hold any of the shared byte budget"
        );
    }

    #[test]
    fn opening_an_edge_lets_the_sinks_frames_through() {
        let mut op = make_test_operator();
        let dest = make_test_destination();
        gate_destinations(&mut op, vec![dest.clone()]);
        op.error_state = Some(Arc::new(RuntimeErrorState::default()));

        open_edges(&op, &[edge_id(1)]);
        assert!(matches!(
            op.try_enqueue_payload(&dest, test_payload(4), false),
            Ok(PayloadEnqueue::Enqueued)
        ));

        // Opening is monotonic and idempotent, so a replay changes nothing.
        open_edges(&op, &[edge_id(1)]);
        assert!(matches!(
            op.try_enqueue_payload(&dest, test_payload(4), false),
            Ok(PayloadEnqueue::Enqueued)
        ));
    }

    #[test]
    fn a_normally_cancelled_edge_discards_its_payload_and_its_pending_chunks() {
        let mut op = make_test_operator();
        let dest = make_test_destination();
        gate_destinations(&mut op, vec![dest.clone()]);
        op.error_state = Some(Arc::new(RuntimeErrorState::default()));
        prepare_test_operator(&mut op);
        open_edges(&op, &[edge_id(1)]);

        op.buffer_chunk(typed_int64_chunk(8)).expect("buffer chunk");
        assert!(op.has_pending_chunks());

        cancel_edge(&op, edge_id(1));
        assert!(matches!(
            op.try_enqueue_payload(&dest, test_payload(4), false),
            Ok(PayloadEnqueue::Discarded)
        ));

        op.flush_pending(true, false).expect("flush pending");
        assert_eq!(op.pending_chunk_count_total(), 0);
        assert_eq!(op.pending_payload_count(), 0);
        assert!(
            !op.has_pending_data(),
            "abandoned data must not hold the sink open"
        );
        assert!(op.send_tracker.is_idle(), "no frame reaches the send queue");
        assert_eq!(op.exchange_queue().inflight_bytes(), 0);
        assert_eq!(
            op.current_error(),
            None,
            "a destination's normal departure is not this producer's failure"
        );
    }

    #[test]
    fn a_normally_cancelled_edge_leaves_the_other_destinations_sending() {
        let mut op = make_test_operator();
        let abandoned = destination_with_finst(1);
        let healthy = destination_with_finst(2);
        gate_destinations(&mut op, vec![abandoned.clone(), healthy.clone()]);
        op.error_state = Some(Arc::new(RuntimeErrorState::default()));
        prepare_test_operator(&mut op);
        open_edges(&op, &[edge_id(1), edge_id(2)]);

        op.buffer_chunk(typed_int64_chunk(8)).expect("buffer chunk");
        assert_eq!(op.pending_per_dest.len(), 2);
        assert!(!op.pending_per_dest[0].is_empty() && !op.pending_per_dest[1].is_empty());

        cancel_edge(&op, edge_id(1));
        op.flush_pending(true, false).expect("flush pending");

        assert!(
            op.pending_per_dest[0].is_empty(),
            "the abandoned edge's chunks are dropped"
        );
        assert!(
            op.pending_per_dest[1].is_empty() && op.pending_payloads_per_dest[1].is_none(),
            "the healthy edge's chunks are handed to the send queue, not dropped or parked"
        );
        assert!(matches!(
            op.try_enqueue_payload(&abandoned, test_payload(4), false),
            Ok(PayloadEnqueue::Discarded)
        ));
        assert!(
            op.edge_gates
                .as_ref()
                .expect("gated sink")
                .gate(edge_id(2))
                .expect("edge two")
                .may_send(),
            "the healthy edge keeps its send permission"
        );
        assert_eq!(op.current_error(), None);
    }

    #[test]
    fn a_closed_edge_does_not_stall_a_destination_that_can_send() {
        let mut op = make_test_operator();
        let closed = destination_with_finst(1);
        let open = destination_with_finst(2);
        gate_destinations(&mut op, vec![closed, open]);
        op.error_state = Some(Arc::new(RuntimeErrorState::default()));
        prepare_test_operator(&mut op);
        open_edges(&op, &[edge_id(2)]);

        op.buffer_chunk(typed_int64_chunk(8)).expect("buffer chunk");
        op.flush_pending(true, false).expect("flush pending");

        assert!(
            op.pending_payloads_per_dest[0].is_some(),
            "the closed edge parks its own payload"
        );
        assert!(
            op.pending_payloads_per_dest[1].is_none() && op.pending_per_dest[1].is_empty(),
            "one edge waiting for permission must not block another edge's flush"
        );
    }

    #[test]
    fn a_destination_no_edge_claims_fails_closed() {
        let mut op = make_test_operator();
        gate_destinations(&mut op, vec![destination_with_finst(1)]);
        op.error_state = Some(Arc::new(RuntimeErrorState::default()));

        let Err(error) = op.try_enqueue_payload(&destination_with_finst(7), test_payload(4), false)
        else {
            panic!("an unattributed destination must not send");
        };
        assert!(
            error
                .detail()
                .contains("not attributed to any outbound edge"),
            "{error}"
        );
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(error.failure_fact().reason().code(), 358);
        assert!(
            matches!(error.cause(), crate::runtime::fragment::ExecutionFailureCause::Domain(cause)
            if cause.downcast_ref::<StreamStepContract>().is_some())
        );
    }

    #[test]
    fn a_closed_edge_holds_at_most_one_payload_and_stops_taking_input() {
        let mut op = make_test_operator();
        let dest = make_test_destination();
        gate_destinations(&mut op, vec![dest]);
        // One byte batches every buffered chunk, so each push offers the
        // closed edge a payload.
        op.max_transmit_batched_bytes = 1;
        let state = RuntimeState::default();

        prepare_test_operator(&mut op);
        let mut pushed = 0;
        while op.input_admissible() && pushed < 8 {
            SinkKernel::consume_for_test(&mut op, &state, typed_int64_chunk(8))
                .expect("push chunk");
            pushed += 1;
        }

        assert!(
            pushed < 8,
            "a closed edge must stop the sink from taking more input"
        );
        assert!(
            op.pending_payload_count() <= op.destinations().len(),
            "the single per-destination payload slot is the whole bound"
        );
        assert_eq!(
            op.exchange_queue().inflight_bytes(),
            0,
            "a closed edge takes nothing from the global or the per-destination ceiling"
        );
        assert!(op.send_tracker.is_idle(), "nothing was handed to the queue");
        let buffered = op.pending_chunk_bytes_total() + op.pending_payload_bytes_total();
        let bound = op.exchange_queue().max_inflight_bytes();
        assert!(
            buffered <= bound,
            "a closed edge held {buffered} bytes, above the send queue's own ceiling {bound}"
        );
        // A healthy destination can still claim the whole budget while the
        // closed edge waits, because the closed edge reserved nothing.
        assert!(op.exchange_queue().can_reserve(bound));

        open_edges(&op, &[edge_id(1)]);
        assert!(
            op.input_admissible(),
            "opening the edge releases the backpressure"
        );
    }

    /// A producer whose input drains before its edge opens is a normal race,
    /// not a failure: the open follows a round trip after every destination
    /// acknowledges creation, and a small fragment can finish reading sooner.
    /// So the finishing drain waits, and the driver is told to come back.
    #[test]
    fn finishing_before_an_edge_opens_waits_instead_of_failing() {
        let runtime = RuntimeState::default();
        let mut op = make_test_operator();
        gate_destinations(&mut op, vec![make_test_destination()]);
        op.error_state = Some(Arc::new(RuntimeErrorState::default()));
        prepare_test_operator(&mut op);
        op.buffer_chunk(typed_int64_chunk(8)).expect("buffer chunk");
        let parked =
            SinkKernel::finish_for_test(&mut op, &runtime).expect("closed edge parks finalization");
        let NextStep::Wait(wait) = parked.next else {
            panic!("closed edge must name its work wait")
        };
        assert!(matches!(wait.reason, WaitReason::Finish));
        assert!(op.current_error().is_none(), "permission is not a failure");
        assert!(op.awaits_edge_permission());
        assert_eq!(
            op.exchange_queue().inflight_bytes(),
            0,
            "closed edge reserved nothing"
        );
        assert!(
            !op.maybe_mark_finished(),
            "buffered output still belongs to this driver"
        );
        open_edges(&op, &[edge_id(1)]);
        assert!(
            wait.is_ready(),
            "edge publication invalidates the exact reported wait"
        );
        assert!(
            matches!(
                op.next_work(op.send_observable.generation()),
                NextStep::Runnable
            ),
            "only the worker can drain the now-admitted payload"
        );
        let completed =
            SinkKernel::poll_for_test(&mut op, &runtime).expect("resume the same finalization");
        assert!(matches!(completed.next, NextStep::Finished));
        assert!(!op.has_pending_data());
        assert!(!op.owes_end_of_stream());
    }

    /// The same race with nothing buffered. Only an end-of-stream is owed, and
    /// forgetting it would leave the destination counting senders forever.
    #[test]
    fn a_closed_edge_owed_only_an_end_of_stream_still_blocks_finishing() {
        let runtime = RuntimeState::default();
        let mut op = make_test_operator();
        gate_destinations(&mut op, vec![make_test_destination()]);
        op.error_state = Some(Arc::new(RuntimeErrorState::default()));
        prepare_test_operator(&mut op);
        assert!(!op.has_pending_data(), "only EOS is owed");
        let parked = SinkKernel::finish_for_test(&mut op, &runtime).expect("signal EOS once");
        let NextStep::Wait(wait) = parked.next else {
            panic!("closed edge must defer its marker")
        };
        assert!(matches!(wait.reason, WaitReason::Finish));
        assert!(!op.maybe_mark_finished());
        assert_eq!(op.shared_order.admitted_count(), 0);
        open_edges(&op, &[edge_id(1)]);
        assert!(wait.is_ready());
        assert_eq!(
            op.shared_order.admitted_count(),
            0,
            "notification cannot send EOS"
        );
        let completed = SinkKernel::poll_for_test(&mut op, &runtime).expect("resume EOS");
        assert!(matches!(completed.next, NextStep::Finished));
        assert_eq!(op.shared_order.admitted_count(), 1);
        assert!(!op.owes_end_of_stream());
    }

    /// The defect this catches: a sink that deferred its end-of-stream while
    /// its edge was closed declared itself *finished* the moment that edge
    /// opened, because "nothing parked and no closed edge" was the whole of
    /// "I owe no more output". The scheduler reads that verdict through
    /// `is_finished` and `need_input` between driver turns, and `set_finishing`
    /// returns early once `finished` is latched -- so the marker was never
    /// sent, by no race: the driver's own turn re-checks `is_finished` before
    /// it re-checks the operator's pending finish.
    ///
    /// The consequence is a hang with no failure anywhere. The receiving
    /// exchange keeps counting senders, so its scan never reaches end of
    /// stream, the root fragment never fills its result buffer, and the
    /// frontend's read completion -- which needs the root FINISHED and its own
    /// end of stream, neither of which fails on absence -- waits out the whole
    /// statement budget. Measured on a real 1FE+3BE cluster as a distributed
    /// `SELECT ... ORDER BY` timing out after 120 s with two minutes of total
    /// silence in every process's log.
    #[test]
    fn an_edge_opening_must_not_finish_a_sink_before_its_end_of_stream_is_sent() {
        let runtime = RuntimeState::default();
        let mut op = make_test_operator();
        op.error_state = Some(Arc::new(RuntimeErrorState::default()));
        gate_destinations(&mut op, vec![make_test_destination()]);
        prepare_test_operator(&mut op);
        let parked =
            SinkKernel::finish_for_test(&mut op, &runtime).expect("closed edge parks the seal");
        let NextStep::Wait(wait) = parked.next else {
            panic!("seal is waiting for its destination")
        };
        assert!(!op.has_pending_data());
        assert_eq!(op.shared_order.admitted_count(), 0);
        open_edges(&op, &[edge_id(1)]);
        assert!(wait.is_ready());
        assert!(matches!(
            op.next_work(op.send_observable.generation()),
            NextStep::Runnable
        ));
        assert_eq!(
            op.shared_order.admitted_count(),
            0,
            "an opened gate alone cannot satisfy the mandatory marker"
        );
        let completed =
            SinkKernel::poll_for_test(&mut op, &runtime).expect("worker sends the seal");
        assert!(matches!(completed.next, NextStep::Finished));
        assert_eq!(op.shared_order.admitted_count(), 1, "one EOS frame");
        assert!(!op.owes_end_of_stream());
        let repeated =
            SinkKernel::poll_for_test(&mut op, &runtime).expect("completed continuation is stable");
        assert!(matches!(repeated.next, NextStep::Finished));
        assert_eq!(op.shared_order.admitted_count(), 1, "EOS is never replayed");
    }

    /// A sink can enter finishing while it still has buffered rows. Once the
    /// asynchronous drain clears them, the scheduler inspects `is_finished`
    /// before giving `set_finishing` another turn. This driver must remain
    /// active until it decrements the shared count and seals its destination.
    #[test]
    fn a_drained_sink_must_count_its_driver_before_finishing() {
        let runtime = RuntimeState::default();
        let mut op = make_test_operator();
        op.error_state = Some(Arc::new(RuntimeErrorState::default()));
        gate_destinations(&mut op, vec![make_test_destination()]);
        prepare_test_operator(&mut op);
        open_edges(&op, &[edge_id(1)]);
        // The continuation after the last local payload drain must still count
        // this driver and publish the one sender-wide marker.
        op.finishing.store(true, Ordering::Release);
        assert!(!op.finish_counted.load(Ordering::SeqCst));
        assert!(!op.has_pending_data());
        assert!(!op.end_of_stream_sent.load(Ordering::SeqCst));
        let completed =
            SinkKernel::poll_for_test(&mut op, &runtime).expect("count and seal in the work step");
        assert!(matches!(completed.next, NextStep::Finished));
        assert!(op.finish_counted.load(Ordering::SeqCst));
        assert!(op.end_of_stream_sent.load(Ordering::SeqCst));
        assert_eq!(op.finish_state.remaining_drivers.load(Ordering::SeqCst), 0);
        assert_eq!(op.shared_order.admitted_count(), 1);
    }

    /// The defect this catches: `set_finishing` counted this driver against
    /// the sink's driver set on every call, while `finishing_wait` had
    /// the driver retry that same call for as long as the edge stayed closed.
    /// One driver's retries therefore consumed the whole set and latched the
    /// one-shot end-of-stream guard, so once the edge finally opened no driver
    /// was the last one any more and end-of-stream was never sent. The
    /// destination then counted senders forever: observed in a real 1FE+3BE
    /// run as a distributed SELECT hanging until its statement deadline.
    #[test]
    fn retrying_set_finishing_while_an_edge_is_closed_does_not_consume_the_driver_set() {
        let state = Arc::new(DataStreamSinkFinishState::default());
        let runtime = RuntimeState::default();
        let mut first = make_test_operator();
        first.finish_state = state.clone();
        first.error_state = Some(Arc::new(RuntimeErrorState::default()));
        gate_destinations(&mut first, vec![make_test_destination()]);
        let mut second = make_test_operator();
        second.driver_id = 1;
        second.finish_state = state.clone();
        second.error_state = Some(Arc::new(RuntimeErrorState::default()));
        gate_destinations(&mut second, vec![make_test_destination()]);
        prepare_test_operator(&mut first);
        prepare_test_operator(&mut second);
        let completed =
            SinkKernel::finish_for_test(&mut first, &runtime).expect("first driver drains");
        assert!(
            matches!(completed.next, NextStep::Finished),
            "a drained non-last driver owes no sender EOS"
        );
        for _ in 0..7 {
            let repeated =
                SinkKernel::poll_for_test(&mut first, &runtime).expect("completed continuation");
            assert!(matches!(repeated.next, NextStep::Finished));
        }
        assert_eq!(
            state.remaining_drivers.load(Ordering::SeqCst),
            1,
            "eight observations count once"
        );
        let parked = SinkKernel::finish_for_test(&mut second, &runtime)
            .expect("last driver owes the marker");
        let NextStep::Wait(wait) = parked.next else {
            panic!("last driver must wait for the exact edge")
        };
        assert_eq!(state.remaining_drivers.load(Ordering::SeqCst), 0);
        assert_eq!(second.shared_order.admitted_count(), 0);
        open_edges(&second, &[edge_id(1)]);
        assert!(wait.is_ready());
        let completed = SinkKernel::poll_for_test(&mut second, &runtime)
            .expect("resume the last driver's seal");
        assert!(matches!(completed.next, NextStep::Finished));
        assert!(!second.owes_end_of_stream());
        assert_eq!(second.shared_order.admitted_count(), 1);
        assert!(
            !state.force_eos_sent.load(Ordering::SeqCst),
            "no false driver-set accounting failure"
        );
    }

    /// The defect this catches: the driver that completes a sink's driver set
    /// sends one end-of-stream for the whole set, but the pending buffers are
    /// per driver. That driver checked only its own, so a sibling that had
    /// finished while still holding rows had them dropped -- the receiver's
    /// sender count closed on the marker and reported the exchange complete,
    /// silently short, with no error anywhere. Observed on a real 1FE+3BE run
    /// as `SELECT ... UNION SELECT ...` returning one row of two and as
    /// `COUNT(*)` over a union returning 0; it reproduced in 3 of 8 suite
    /// runs and in none on the commit before the cutover.
    #[test]
    fn a_driver_still_holding_rows_is_not_counted_against_the_driver_set() {
        let state = Arc::new(DataStreamSinkFinishState::default());
        // Two drivers on one sink, both sharing the frozen finish state.
        let runtime = RuntimeState::default();

        let mut holder = make_test_operator();
        holder.finish_state = Arc::clone(&state);
        holder.error_state = Some(Arc::new(RuntimeErrorState::default()));
        gate_destinations(&mut holder, vec![make_test_destination()]);

        let mut sibling = make_test_operator();
        sibling.driver_id = 1;
        sibling.finish_state = Arc::clone(&state);
        sibling.error_state = Some(Arc::new(RuntimeErrorState::default()));
        gate_destinations(&mut sibling, vec![make_test_destination()]);
        prepare_test_operator(&mut holder);
        prepare_test_operator(&mut sibling);

        // One driver finishes holding rows it has not been able to hand over.
        holder
            .buffer_chunk(typed_int64_chunk(8))
            .expect("buffer chunk");
        assert!(holder.has_pending_chunks());
        SinkKernel::finish_for_test(&mut holder, &runtime)
            .expect("a closed edge parks the drain rather than failing");
        assert!(
            holder.has_pending_data(),
            "the closed edge leaves the rows on this driver"
        );
        assert_eq!(
            state.remaining_drivers.load(Ordering::SeqCst),
            2,
            "a driver holding rows has not finished, so it must not count"
        );

        // Its sibling finishing must therefore not complete the set, and must
        // not seal the sender over rows the first driver still holds.
        SinkKernel::finish_for_test(&mut sibling, &runtime)
            .expect("the sibling drains with nothing to hand over");
        assert_eq!(state.remaining_drivers.load(Ordering::SeqCst), 1);
        assert!(
            !sibling.end_of_stream_sent.load(Ordering::SeqCst),
            "no end-of-stream may go out while a sibling still holds rows"
        );

        // The edge opens: the holder hands its rows over on its next turn,
        // counts, completes the set, and only then does the marker follow the
        // frames it seals.
        open_edges(&holder, &[edge_id(1)]);
        SinkKernel::finish_for_test(&mut holder, &runtime)
            .expect("the parked rows and then the seal go out");
        assert!(!holder.has_pending_data());
        assert_eq!(state.remaining_drivers.load(Ordering::SeqCst), 0);
        assert!(
            holder.end_of_stream_sent.load(Ordering::SeqCst),
            "the driver that counted last seals, and it seals after its rows"
        );
        assert!(
            !state.force_eos_sent.load(Ordering::SeqCst),
            "the one-shot guard is for a genuine accounting error, not for this"
        );
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
            &[novarocks_types::SlotId::new(1)],
        )
        .unwrap();
        let chunk = Chunk::new_with_chunk_schema(batch, chunk_schema);

        let partitions =
            partition_chunk_by_hash_arrays(&chunk, &[array], 2, false).expect("partition");

        assert_eq!(
            partitions
                .iter()
                .map(|chunk| chunk.record_batch().num_rows())
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
        assert_eq!(out[0].record_batch().num_rows(), 8);
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
        let total_rows: usize = out.iter().map(|c| c.record_batch().num_rows()).sum();
        assert_eq!(total_rows, rows);
        for piece in &out {
            assert!(
                piece.record_batch().num_rows() > 0,
                "slice produced zero-row chunk"
            );
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
        assert_eq!(out[0].record_batch().num_rows(), 1);
    }

    mod typed_sender_header_tests {
        use super::*;
        use crate::exec::chunk::{ArrayNormalizationStep, ArrayPathStep};
        use crate::exec::pipeline::operator::{
            ConstructionFailure, InputConstructionFactory, InputConstructionOwner,
        };
        use crate::runtime::fragment::ExecutionFailureCause;
        use crate::runtime::mem_tracker::MemTracker;
        use arrow::array::ListArray;
        use arrow::buffer::{OffsetBuffer, ScalarBuffer};
        use novarocks_type_contract::{
            TypedValueBinding, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef,
            ValueTypeStorageLifetime,
        };

        fn actual_factory(tracker: Arc<MemTracker>) -> Arc<dyn InputConstructionFactory> {
            crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
                crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
                tracker, Arc::new(RuntimeErrorState::default()),
            ).unwrap()
        }
        fn source(
            array: ArrayRef,
            value: ValueTypeRef,
            rows: usize,
            factory: &dyn InputConstructionFactory,
        ) -> Chunk {
            let pure = ValueTypeStorageLifetime::pure();
            let binding = TypedValueBinding::try_new(
                value,
                ValueLayout::try_new(array.data_type(), pure.clone()).unwrap(),
            )
            .unwrap();
            let slot = ChunkSlotSchema::try_new_typed(
                SlotId::new(91),
                Arc::new(Field::new("value", array.data_type().clone(), false)),
                binding,
                None,
                pure.clone(),
            )
            .unwrap();
            let schema = Arc::new(ChunkSchema::try_new_typed(vec![slot], pure).unwrap());
            let owner = factory.begin_typed_construction(&schema).unwrap();
            owner
                .observe(
                    0,
                    &[],
                    ArrayNormalizationStep::CreateChunkColumns { channels: 1 },
                )
                .unwrap();
            Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
                schema,
                vec![array],
                rows,
                owner.storage_lifetime(),
                |step| owner.observe(0, &[], step),
            )
            .unwrap()
        }
        fn plain(factory: &dyn InputConstructionFactory) -> Chunk {
            let value = ValueTypeRef::try_new(
                ValueKind::Scalar(DataType::Int32),
                false,
                ValueTypeStorageLifetime::pure(),
            )
            .unwrap();
            source(
                Arc::new(Int32Array::from((0..8).collect::<Vec<_>>())),
                value,
                8,
                factory,
            )
        }
        fn nested_dictionary(factory: &dyn InputConstructionFactory) -> Chunk {
            // The unused NULL value is legal: only valid keys 0 and 2 are selected.
            let values = Arc::new(StringArray::from(vec![Some("{}"), None, Some("[]")]));
            let dictionary: ArrayRef = Arc::new(
                DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0, 2, 0, 2]), values)
                    .unwrap(),
            );
            #[allow(deprecated)]
            let item = Arc::new(Field::new_dict(
                "item",
                dictionary.data_type().clone(),
                true,
                291,
                false,
            ));
            let list: ArrayRef = Arc::new(
                ListArray::try_new(
                    item,
                    OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1, 2, 3, 4])),
                    dictionary,
                    None,
                )
                .unwrap(),
            );
            let pure = ValueTypeStorageLifetime::pure();
            let json = ValueTypeRef::try_new(
                ValueKind::Nominal(ValueLogicalType::Json),
                true,
                pure.clone(),
            )
            .unwrap();
            let value = ValueTypeRef::try_new(ValueKind::List(json), false, pure).unwrap();
            source(list, value, 4, factory)
        }
        fn install_and_prepare(
            factory: Arc<dyn InputConstructionFactory>,
        ) -> DataStreamSinkOperator {
            let mut sink = make_test_operator();
            sink.set_construction_factory(factory);
            sink.prepare().unwrap();
            sink
        }
        #[test]
        fn oversized_typed_plain_keeps_original_values_and_last_header_hold() {
            let tracker = MemTracker::new_root("sender typed plain slice");
            let factory = actual_factory(tracker.clone());
            let source = plain(factory.as_ref());
            let sink = install_and_prepare(factory.clone());
            let out = split_oversized_chunks_with_creation(
                vec![source.clone()],
                1,
                sink.construction_factory.as_ref().unwrap().as_ref(),
            )
            .unwrap();
            assert_eq!(out.chunks.len(), 8);
            for (row, piece) in out.chunks.iter().enumerate() {
                assert_eq!(piece.len(), 1);
                assert!(std::ptr::eq(piece.chunk_schema(), source.chunk_schema()));
                let values = piece.columns()[0]
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                assert_eq!(values.value(0), row as i32);
                assert_eq!(values.null_count(), 0);
            }
            let retained = out.chunks[3].clone();
            drop(out);
            drop(source);
            drop(sink);
            drop(factory);
            assert!(
                tracker.current() > 0,
                "the actual independent slice header/source holds remain"
            );
            assert_eq!(retained.len(), 1);
            drop(retained);
            assert_eq!(tracker.current(), 0);
        }
        #[test]
        fn oversized_typed_nested_dictionary_keeps_json_null_and_encoding_contract() {
            let tracker = MemTracker::new_root("sender typed nested dictionary slice");
            let factory = actual_factory(tracker.clone());
            let source = nested_dictionary(factory.as_ref());
            let sink = install_and_prepare(factory.clone());
            let out = split_oversized_chunks_with_creation(
                vec![source.clone()],
                1,
                sink.construction_factory.as_ref().unwrap().as_ref(),
            )
            .unwrap();
            assert_eq!(out.chunks.len(), 4);
            for (row, piece) in out.chunks.iter().enumerate() {
                assert_eq!(piece.len(), 1);
                let value = piece.chunk_schema().slots()[0].typed_binding().value_type();
                assert!(!value.nullable());
                let ValueKind::List(item) = value.kind() else {
                    panic!("complete List declaration")
                };
                assert!(matches!(
                    item.kind(),
                    ValueKind::Nominal(ValueLogicalType::Json)
                ));
                assert!(
                    item.nullable(),
                    "the genuine Arrow child declaration permits the retained dictionary NULL"
                );
                let list = piece.columns()[0]
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap();
                let dictionary = list
                    .values()
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()
                    .unwrap();
                let child = dictionary
                    .values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let index = list.value_offsets()[0] as usize;
                assert_eq!(
                    child.value(dictionary.keys().value(index) as usize),
                    if row % 2 == 0 { "{}" } else { "[]" }
                );
                assert_eq!(
                    child.null_count(),
                    1,
                    "the genuine unreferenced dictionary NULL is retained"
                );
                let DataType::List(field) = piece.columns()[0].data_type() else {
                    panic!("actual List carrier")
                };
                #[allow(deprecated)]
                {
                    assert_eq!(field.dict_id(), Some(291));
                    assert_eq!(field.dict_is_ordered(), Some(false));
                }
            }
            drop(out);
            drop(source);
            drop(sink);
            drop(factory);
            assert_eq!(tracker.current(), 0);
        }

        // Test instrumentation delegates every request and lifetime to the real
        // installed tracker factory; it supplies no grant or replacement model.
        struct FirstHeaderRefusalFactory {
            inner: Arc<dyn InputConstructionFactory>,
            tracker: Arc<MemTracker>,
            requests: Arc<AtomicUsize>,
            later: Arc<AtomicUsize>,
        }
        struct FirstHeaderRefusalOwner {
            inner: Box<dyn InputConstructionOwner>,
            tracker: Arc<MemTracker>,
            requests: Arc<AtomicUsize>,
            later: Arc<AtomicUsize>,
        }
        impl InputConstructionOwner for FirstHeaderRefusalOwner {
            fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
                self.inner.storage_lifetime()
            }
            fn request(&self, layout: std::alloc::Layout) -> Result<(), ConstructionFailure> {
                if self.requests.fetch_add(1, Ordering::SeqCst) == 0 {
                    self.tracker
                        .install_limit_once(self.tracker.current() + 1)
                        .unwrap();
                }
                self.inner.request(layout)
            }
            fn observe_opaque_upper_bound(&self, bytes: usize) -> Result<(), ConstructionFailure> {
                self.later.fetch_add(1, Ordering::SeqCst);
                self.inner.observe_opaque_upper_bound(bytes)
            }
            fn observe(
                &self,
                ordinal: usize,
                path: &[ArrayPathStep],
                step: ArrayNormalizationStep<'_>,
            ) -> Result<(), ConstructionFailure> {
                self.later.fetch_add(1, Ordering::SeqCst);
                self.inner.observe(ordinal, path, step)
            }
        }
        impl InputConstructionFactory for FirstHeaderRefusalFactory {
            fn observe_input_work(&self, units: usize) -> Result<(), ConstructionFailure> {
                self.later.fetch_add(1, Ordering::SeqCst);
                self.inner.observe_input_work(units)
            }
            fn begin_typed_construction(
                &self,
                schema: &ChunkSchema,
            ) -> Result<Box<dyn InputConstructionOwner>, ConstructionFailure> {
                Ok(Box::new(FirstHeaderRefusalOwner {
                    inner: self.inner.begin_typed_construction(schema)?,
                    tracker: self.tracker.clone(),
                    requests: self.requests.clone(),
                    later: self.later.clone(),
                }))
            }
            fn begin_compiled_layout_construction(
                &self,
                layout: &novarocks_local_program::StaticLayout,
            ) -> Result<Box<dyn InputConstructionOwner>, ConstructionFailure> {
                self.inner.begin_compiled_layout_construction(layout)
            }
        }
        #[test]
        fn oversized_typed_first_header_refusal_is_original_memory_and_no_tail() {
            let tracker = MemTracker::new_root("sender first split header refusal");
            let factory = actual_factory(tracker.clone());
            let source = plain(factory.as_ref());
            let requests = Arc::new(AtomicUsize::new(0));
            let later = Arc::new(AtomicUsize::new(0));
            let refusing = Arc::new(FirstHeaderRefusalFactory {
                inner: factory.clone(),
                tracker: tracker.clone(),
                requests: requests.clone(),
                later: later.clone(),
            });
            let sink = install_and_prepare(refusing);
            let baseline = tracker.current();
            let refusal = split_oversized_chunks_with_creation(
                vec![source.clone()],
                1,
                sink.construction_factory.as_ref().unwrap().as_ref(),
            )
            .err()
            .expect("original first Vec header request refused");
            assert!(matches!(
                refusal.cause(),
                ExecutionFailureCause::Construction(ConstructionFailure::Memory(_))
            ));
            assert_eq!(requests.load(Ordering::SeqCst), 1);
            assert_eq!(
                later.load(Ordering::SeqCst),
                0,
                "no measurement, slicing, completion or opaque tail after refusal"
            );
            assert_eq!(tracker.current(), baseline);
            assert_eq!(source.len(), 8);
            assert_eq!(
                source.columns()[0]
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .value(7),
                7
            );
            drop(source);
            drop(sink);
            drop(factory);
            assert_eq!(tracker.current(), 0);
        }
        #[test]
        fn sender_missing_actual_factory_refuses_before_input_consumption() {
            let tracker = MemTracker::new_root("sender missing factory source");
            let factory = actual_factory(tracker.clone());
            let source = plain(factory.as_ref());
            let mut sink = make_test_operator();
            let refusal = sink.prepare().unwrap_err();
            assert!(matches!(
                refusal.cause(),
                ExecutionFailureCause::Construction(ConstructionFailure::MissingOwner)
            ));
            assert!(!sink.prepared);
            let state = RuntimeState::for_local_execution(None, None, None, None, None, None, None);
            let refusal = sink.consume_for_test(&state, source.clone()).unwrap_err();
            assert!(matches!(
                refusal.cause(),
                ExecutionFailureCause::Construction(ConstructionFailure::MissingOwner)
            ));
            assert_eq!(sink.pending_chunk_count_total(), 0);
            assert_eq!(sink.shared_order.admitted_count(), 0);
            assert!(sink.send_tracker.is_idle());
            assert_eq!(source.len(), 8);
            drop(source);
            drop(factory);
            assert_eq!(tracker.current(), 0);
        }
    }

    // This fixture enters through the real frozen-package/local-compiler chain.
    // Its roots are explicit test-only Project outputs: it opens no production
    // write router or Task binding constructor.
    mod conditional_compiled_selection {
        use super::*;
        use arrow::{array::Int64Array, record_batch::RecordBatch};
        use novarocks_connector_contract::PureProviderProgramCatalog;
        use novarocks_functions::{
            ConstantPolicy, EngineFunctionCatalogBuilder, FunctionKind, InstalledPureKernel,
            PureImplementationDeclaration, PureImplementationId, PureKernelAbi,
        };
        use novarocks_local_compiler::{
            LocalCompileOptions, compile_fragment, validate_fragment_providers,
        };
        use novarocks_local_program::{KernelAbiVersion, ProgramNodeExpressionRole, ProgramNodeId};
        use novarocks_physical_plan::{
            BinaryOperator, ConstantPools, ExprId, ExprKind, Fragment, FragmentBuilder,
            FragmentCuts, FragmentId, FragmentPackage, FragmentPackageAdmission,
            FragmentPackageInput, FragmentSink, FrozenFragmentCalls, FrozenFragmentPruning,
            LiteralValue, NodeId, PhysicalExpressionRoots, PhysicalRootUses, PipelineDopDomain,
            PlanLimits, PlanVersionId, PropertyProofProjectionLimits, RequiredContracts,
            ResultField, ResultPort, ResultValueDomain, StagePlacementPolicy, ValueId, ValueOrigin,
        };
        use novarocks_type_contract::{
            CompileControlError, CompilePhase, ControlShape, DecimalOverflowPolicy,
            EvaluationDemand, EvaluationDomainId, ExpressionControlFlow, ExpressionEffectContext,
            ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId, FunctionValueType,
            PureCompileControl, SemanticParameterId, SemanticParameterKey, SemanticParameterRef,
            SemanticParameterValue, SemanticParameters,
        };
        use std::{collections::BTreeMap, num::NonZeroUsize};

        struct FixtureControl;
        impl PureCompileControl for FixtureControl {
            fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
                Ok(())
            }
        }

        fn root(expression: u32) -> ProgramExpressionRootSite {
            ProgramExpressionRootSite::Node {
                node: ProgramNodeId::new(2),
                role: ProgramNodeExpressionRole::ProjectOutput { expression },
            }
        }

        fn visit(
            fragment: &Fragment,
            definition: ExprId,
            demand: EvaluationDemand,
            ordered: &mut Vec<ExpressionInvocation<ExprId>>,
        ) -> ExpressionUseId {
            let arguments = match &fragment.expressions().get(definition).unwrap().kind {
                ExprKind::Binary { left, right, .. } => [*left, *right]
                    .into_iter()
                    .map(|child| visit(fragment, child, EvaluationDemand::Value, ordered))
                    .collect::<Vec<_>>(),
                ExprKind::Value(_) | ExprKind::Literal(LiteralValue::Null) => Vec::new(),
                other => panic!("unexpected conditional exchange fixture expression: {other:?}"),
            };
            let use_id = ExpressionUseId::new(u32::try_from(ordered.len()).unwrap());
            ordered.push(ExpressionInvocation {
                context: ExpressionEffectContext {
                    use_id,
                    domain: EvaluationDomainId::new(0),
                    demand,
                },
                definition,
                control: ControlShape::Eager,
                arguments: arguments.into_boxed_slice(),
            });
            use_id
        }

        fn program() -> Arc<LocalProgram> {
            let fragment_id = FragmentId::new(219);
            let source = NodeId::new(10);
            let input = NodeId::new(20);
            let output = NodeId::new(30);
            let integer = FunctionValueType::new(DataType::Int64, true);
            let boolean = FunctionValueType::new(DataType::Boolean, true);
            let mut builder = FragmentBuilder::new(fragment_id);
            builder
                .add_values(source, Box::from([Box::default()]), Box::default())
                .unwrap();
            let mut inputs = Vec::new();
            for ty in [&integer, &integer, &boolean] {
                let expr = builder
                    .add_expression(input, ty.clone(), ExprKind::Literal(LiteralValue::Null))
                    .unwrap();
                let value = builder
                    .add_value(ty.clone(), ValueOrigin::Expr { node: input, expr })
                    .unwrap();
                inputs.push((expr, value));
            }
            let input_ids = inputs.iter().map(|(_, id)| *id).collect::<Vec<ValueId>>();
            builder
                .add_project(
                    input,
                    source,
                    inputs.into_boxed_slice(),
                    input_ids.clone().into_boxed_slice(),
                )
                .unwrap();
            let left = builder
                .add_expression(output, integer.clone(), ExprKind::Value(input_ids[0]))
                .unwrap();
            let right = builder
                .add_expression(output, integer.clone(), ExprKind::Value(input_ids[1]))
                .unwrap();
            let parameter = SemanticParameterRef {
                id: SemanticParameterId::new(0),
                expected_key: SemanticParameterKey::AllowThrowException,
            };
            let key = builder
                .add_expression(
                    output,
                    integer.clone(),
                    ExprKind::Binary {
                        op: BinaryOperator::Add,
                        left,
                        right,
                        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                        allow_throw_exception: Some(parameter),
                    },
                )
                .unwrap();
            let applies = builder
                .add_expression(output, boolean.clone(), ExprKind::Value(input_ids[2]))
                .unwrap();
            let mut outputs = Vec::new();
            let mut fields = Vec::new();
            for (expr, ty, name) in [(key, integer, "key"), (applies, boolean, "applies")] {
                let value = builder
                    .add_value(ty.clone(), ValueOrigin::Expr { node: output, expr })
                    .unwrap();
                outputs.push((expr, value));
                fields.push(ResultField {
                    domain: ResultValueDomain::Plain,
                    name: name.into(),
                    alias: None,
                    value,
                    ty,
                });
            }
            let output_ids = outputs.iter().map(|(_, id)| *id).collect::<Vec<_>>();
            builder
                .add_project(
                    output,
                    input,
                    outputs.into_boxed_slice(),
                    output_ids.into_boxed_slice(),
                )
                .unwrap();
            let fragment = builder
                .finish_definition(
                    output,
                    FragmentSink::Result,
                    PipelineDopDomain {
                        min: 1,
                        max: 1,
                        requires_power_of_two: false,
                    },
                    StagePlacementPolicy::Singleton,
                )
                .unwrap();
            let roots = PhysicalExpressionRoots::try_new(&fragment, &FixtureControl).unwrap();
            let mut ordered = Vec::new();
            let bindings = roots
                .sites()
                .iter()
                .map(|(site, expression)| {
                    (
                        *site,
                        visit(&fragment, expression.expr, expression.demand, &mut ordered),
                    )
                })
                .collect();
            let flow = ExpressionControlFlow::try_new(
                vec![ExpressionEvaluationDomain {
                    id: EvaluationDomainId::new(0),
                    parent: None,
                    guard: None,
                }],
                ordered,
                fragment.expressions(),
                CompilePhase::Validate,
                &FixtureControl,
            )
            .unwrap();
            let uses =
                PhysicalRootUses::try_new(&fragment, flow, bindings, &FixtureControl).unwrap();
            let calls =
                FrozenFragmentCalls::try_new(&fragment, &uses, vec![], &FixtureControl).unwrap();
            let result = ResultPort {
                scalar_schema: None,
                fragment: fragment_id,
                output: fragment.nodes()[&output].output.clone(),
                fields: fields.into_boxed_slice(),
            };
            let package = Arc::new(
                FragmentPackage::try_new(
                    FragmentPackageInput {
                        version: PlanVersionId::try_new([219; 16]).unwrap(),
                        required: RequiredContracts::default(),
                        constants: ConstantPools::empty(),
                        fragment,
                        expression_uses: uses,
                        calls,
                        pruning: FrozenFragmentPruning::try_new(
                            fragment_id,
                            vec![],
                            &FixtureControl,
                        )
                        .unwrap(),
                        cuts: FragmentCuts::default(),
                        result: Some(result),
                        parameters: SemanticParameters::try_new([(
                            parameter.id,
                            SemanticParameterValue::AllowThrowException(false),
                        )])
                        .unwrap(),
                        scans: BTreeMap::new(),
                        writes: BTreeMap::new(),
                        annotations: Box::default(),
                    },
                    // Explicit bounded fixture invoices; no production defaults.
                    FragmentPackageAdmission {
                        plan_limits: PlanLimits::FROZEN,
                        source_retained_bytes: 64 * 1024 * 1024,
                        property_projection_limits: PropertyProofProjectionLimits {
                            max_request_bytes: 16 * 1024 * 1024,
                            max_coexisting_bytes: 256 * 1024 * 1024,
                            max_projection_work: 16 * 1024 * 1024,
                        },
                    },
                    &FixtureControl,
                )
                .unwrap(),
            );
            let providers =
                PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &FixtureControl)
                    .unwrap();
            // The compiler requires a nonempty sealed catalogue even when all
            // roots are intrinsic. Seal the real IF owner, never a mock evaluator.
            let actual = novarocks_functions::builtin::catalogue::builtin_engine_function_catalog();
            let mut catalogue = EngineFunctionCatalogBuilder::new();
            catalogue
                .register(
                    actual
                        .definition("if", FunctionKind::Scalar)
                        .unwrap()
                        .clone(),
                )
                .unwrap();
            let functions = catalogue
                .seal_pure([InstalledPureKernel {
                    function: novarocks_type_contract::FunctionId::try_new("builtin.scalar/if/v1")
                        .unwrap(),
                    kind: FunctionKind::Scalar,
                    implementation: PureImplementationDeclaration {
                        overload: novarocks_type_contract::FunctionOverloadId::try_new(
                            "builtin.scalar/if/(bool,any<T>,any<T>)->any<T>;widen;legacy",
                        )
                        .unwrap(),
                        implementation: PureImplementationId::try_new(
                            "builtin.scalar/if/selected-v1",
                        )
                        .unwrap(),
                        abi: PureKernelAbi::ControlIntrinsicV1,
                    },
                    aggregate_state_format: None,
                }])
                .unwrap();
            Arc::new(
                compile_fragment(
                    validate_fragment_providers(package, &providers, &FixtureControl).unwrap(),
                    &functions,
                    LocalCompileOptions {
                        pipeline_dop: NonZeroUsize::MIN,
                        root_sink_dop: Some(NonZeroUsize::MIN),
                        kernel_abi: KernelAbiVersion::CURRENT,
                        exchange_wait: Duration::from_secs(120),
                        constants: ConstantPolicy {
                            max_rows: 16,
                            max_array_nodes: 128,
                            max_logical_elements: 1024,
                            max_retained_buffer_bytes: 1 << 20,
                            max_type_depth: 64,
                            max_type_nodes: 4096,
                            max_dictionary_depth: 64,
                            max_metadata_bytes: 1 << 20,
                            max_library_validation_work: 1 << 20,
                            max_library_validation_bytes: 1 << 20,
                        },
                    },
                    &FixtureControl,
                )
                .unwrap(),
            )
        }

        fn input(program: &LocalProgram, applies: Vec<Option<bool>>, left: Vec<i64>) -> Chunk {
            let rows = applies.len();
            assert_eq!(left.len(), rows);
            let layout = program.graph().nodes()[1].output_layout();
            let batch = RecordBatch::try_new(
                Arc::clone(layout.schema()),
                vec![
                    Arc::new(Int64Array::from(left)),
                    Arc::new(Int64Array::from(vec![1; rows])),
                    Arc::new(BooleanArray::from(applies)),
                ],
            )
            .unwrap();
            Chunk::try_new_with_chunk_schema(
                batch,
                ChunkSchema::from_compiled_layout(layout).unwrap(),
            )
            .unwrap()
        }

        fn sink(program: Arc<LocalProgram>) -> DataStreamSinkOperator {
            let mut sink = make_test_operator();
            sink.compiled_partition = Some(CompiledPartitionState {
                keys: CompiledPartitionKeys::conditional(program, vec![root(0)], root(1)),
                instances: None,
                applicability_instances: None,
            });
            prepare_test_operator(&mut sink);
            sink
        }

        fn evaluate(
            sink: &mut DataStreamSinkOperator,
            chunk: &Chunk,
        ) -> ExecutionResult<CompiledPartitionBatch> {
            let control = RuntimeErrorState::default();
            let mut work =
                crate::exec::pipeline::step_context::TurnWork::new(std::num::NonZeroU64::MIN, 64);
            let mut conversions = novarocks_value_arrays::decode::ValueConversionContext::default();
            let ctx = StepContext::new(&control, &mut work, &mut conversions);
            sink.compiled_partition_keys(&ctx, chunk)
                .map(|batch| batch.expect("compiled partition"))
        }

        #[test]
        fn conditional_compiled_keys_skip_false_and_null_overflow_rows() {
            let program = program();
            let chunk = input(
                &program,
                vec![Some(false), Some(true), None],
                vec![i64::MAX, 6, i64::MAX],
            );
            // Positive control: this real compiled key fails if any excluded
            // payload row is accidentally evaluated across the original batch.
            let mut full_sink = sink(program.clone());
            let mut instances = None;
            let control = full_sink.compiled_control.as_ref().unwrap();
            compiled_instances(&mut instances, &program, &[root(0)], control).unwrap();
            let error = evaluate_all(
                &mut instances.as_mut().unwrap()[0],
                root(0),
                chunk.record_batch(),
                control,
            )
            .expect_err("signed addition must overflow on excluded row 0");
            assert!(error.to_string().contains("batch row 0"), "{error}");
            assert!(
                error.to_string().to_ascii_lowercase().contains("overflow"),
                "{error}"
            );
            // Applicability and key instances are owned by the real sink path.
            let batch = evaluate(&mut full_sink, &chunk).unwrap();
            assert_eq!(batch.selected_rows.as_deref(), Some([1].as_slice()));
            assert_eq!(batch.route, CompiledPartitionRoute::ConditionalKeyV1);
            assert_eq!(batch.arrays.len(), 1);
            let keys = batch.arrays[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys.value(0), 7);
            assert_eq!(keys.null_count(), 0);
            let compiled = full_sink.compiled_partition.as_ref().unwrap();
            assert_eq!(compiled.applicability_instances.as_ref().unwrap().len(), 1);
            assert_eq!(compiled.instances.as_ref().unwrap().len(), 1);
        }

        #[test]
        fn conditional_compiled_all_false_or_null_skips_key_instantiation() {
            let program = program();
            let chunk = input(
                &program,
                vec![Some(false), None, Some(false)],
                vec![i64::MAX; 3],
            );
            let mut sink = sink(program);
            let batch = evaluate(&mut sink, &chunk).unwrap();
            assert_eq!(batch.selected_rows.as_deref(), Some([].as_slice()));
            assert!(batch.arrays.is_empty());
            assert!(batch.types.is_empty());
            let compiled = sink.compiled_partition.as_ref().unwrap();
            assert!(compiled.applicability_instances.is_some());
            assert!(
                compiled.instances.is_none(),
                "inactive key must not even be instantiated"
            );
        }

        #[test]
        fn conditional_compiled_true_overflow_remains_a_required_batch_row_error() {
            let program = program();
            let chunk = input(
                &program,
                vec![Some(false), None, Some(true)],
                vec![i64::MAX; 3],
            );
            let mut sink = sink(program);
            let error = match evaluate(&mut sink, &chunk) {
                Ok(_) => panic!("selected overflow must fail"),
                Err(error) => error,
            };
            let message = error.to_string();
            assert!(
                message.contains("batch row 2 (selected ordinal 0)"),
                "{message}"
            );
            assert!(
                message.to_ascii_lowercase().contains("overflow"),
                "{message}"
            );
        }
    }

    fn routing_batch(
        source: &Chunk,
        route: CompiledPartitionRoute,
        selected_rows: Option<Vec<usize>>,
        values: Vec<Option<i64>>,
    ) -> CompiledPartitionBatch {
        let mut sink = make_test_operator();
        prepare_test_operator(&mut sink);
        let owner = sink
            .construction_factory
            .as_ref()
            .unwrap()
            .begin_input_construction(source)
            .unwrap();
        let (arrays, types) = if values.is_empty() {
            (Vec::new(), Vec::new())
        } else {
            (
                vec![Arc::new(arrow::array::Int64Array::from(values)) as ArrayRef],
                vec![novarocks_type_contract::FunctionValueType::new(
                    DataType::Int64,
                    true,
                )],
            )
        };
        CompiledPartitionBatch {
            arrays,
            types,
            selected_rows,
            route,
            owner,
        }
    }

    #[test]
    fn conditional_network_routes_true_keys_and_round_robins_false_or_null_rows() {
        let source = typed_int64_chunk(6);
        let state = RuntimeState::default();
        let control = state.error_state();
        let mut work =
            crate::exec::pipeline::step_context::TurnWork::new(std::num::NonZeroU64::MIN, 64);
        let mut conversions = novarocks_value_arrays::decode::ValueConversionContext::default();
        let mut ctx = StepContext::new(&control, &mut work, &mut conversions);
        let mask = BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
        ]);
        let batch = routing_batch(
            &source,
            CompiledPartitionRoute::ConditionalKeyV1,
            Some(vec![0, 3]),
            vec![Some(7), Some(7)],
        );
        assert_eq!(
            selected_partition_rows(&mask, 6, batch.owner.as_ref(), &ctx).unwrap(),
            vec![0, 3]
        );
        let mut cursor = 0;
        let rows = instance_partition_rows(&batch, &source, 6, 3, &mut cursor, &mut ctx).unwrap();
        // Frozen native tuple(7) = 0xba4003266beeb280; InstanceV1 modulo 3 = 0.
        // TRUE 0/3 share that owner; FALSE/NULL 1/2/4/5 use 0/1/2/0.
        assert_eq!(rows, vec![vec![0, 1, 3, 5], vec![2], vec![4]]);
        assert_eq!(cursor, 1);
        let next = routing_batch(
            &source,
            CompiledPartitionRoute::ConditionalKeyV1,
            Some(Vec::new()),
            Vec::new(),
        );
        assert_eq!(
            instance_partition_rows(&next, &source, 2, 3, &mut cursor, &mut ctx).unwrap(),
            vec![vec![], vec![0], vec![1]]
        );
        assert_eq!(cursor, 0, "per-producer bypass continues across batches");
    }

    #[test]
    fn all_bypass_needs_no_key_column_or_key_decode_and_wraps_its_cursor() {
        let source = typed_int64_chunk(5);
        let batch = routing_batch(
            &source,
            CompiledPartitionRoute::ConditionalKeyV1,
            Some(Vec::new()),
            Vec::new(),
        );
        let state = RuntimeState::default();
        let control = state.error_state();
        let mut work =
            crate::exec::pipeline::step_context::TurnWork::new(std::num::NonZeroU64::MIN, 64);
        let mut conversions = novarocks_value_arrays::decode::ValueConversionContext::default();
        let mut ctx = StepContext::new(&control, &mut work, &mut conversions);
        let mut cursor = 2;
        assert_eq!(
            instance_partition_rows(&batch, &source, 5, 3, &mut cursor, &mut ctx).unwrap(),
            vec![vec![1, 4], vec![2], vec![0, 3]]
        );
        assert_eq!(cursor, 1);
    }

    #[test]
    fn selected_network_target_key_cannot_acquire_the_null_identity() {
        use novarocks_failure_contract::FailureProjection;
        let source = typed_int64_chunk(3);
        let batch = routing_batch(
            &source,
            CompiledPartitionRoute::ConditionalKeyV1,
            Some(vec![1]),
            vec![None],
        );
        let state = RuntimeState::default();
        let control = state.error_state();
        let mut work =
            crate::exec::pipeline::step_context::TurnWork::new(std::num::NonZeroU64::MIN, 64);
        let mut conversions = novarocks_value_arrays::decode::ValueConversionContext::default();
        let mut ctx = StepContext::new(&control, &mut work, &mut conversions);
        let failure = instance_partition_rows(&batch, &source, 3, 4, &mut 0, &mut ctx).unwrap_err();
        let expected =
            ExecutionFailure::from_domain(novarocks_value_arrays::ValueError::Invariant(
                novarocks_value_arrays::ValueViolation::RequiredNull,
            ));
        assert_eq!(failure.failure_fact(), expected.failure_fact());
    }

    #[test]
    fn hierarchical_network_uses_frozen_instance_domain_and_keeps_ordinary_mapping() {
        let source = typed_int64_chunk(3);
        let batch = routing_batch(
            &source,
            CompiledPartitionRoute::HierarchicalV1,
            None,
            vec![Some(1), Some(2), Some(3)],
        );
        let state = RuntimeState::default();
        let control = state.error_state();
        let mut work =
            crate::exec::pipeline::step_context::TurnWork::new(std::num::NonZeroU64::MIN, 64);
        let mut conversions = novarocks_value_arrays::decode::ValueConversionContext::default();
        let mut ctx = StepContext::new(&control, &mut work, &mut conversions);
        assert_eq!(
            instance_partition_rows(&batch, &source, 3, 4, &mut 0, &mut ctx).unwrap(),
            vec![vec![], vec![0], vec![1], vec![2]]
        );
        // Ordinary FNV modulo 4 remains [2, 1, 0], with no level mixing.
        let ordinary = partition_chunk_by_hash_arrays(&source, &batch.arrays, 4, false).unwrap();
        let values = |chunk: &Chunk| {
            if chunk.is_empty() {
                vec![]
            } else {
                chunk.columns()[0]
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            }
        };
        assert_eq!(
            ordinary.iter().map(values).collect::<Vec<_>>(),
            vec![vec![2], vec![1], vec![0], vec![]]
        );
    }

    #[test]
    fn network_partition_scope_preserves_the_exact_originating_stop() {
        let source = typed_int64_chunk(2);
        let batch = routing_batch(
            &source,
            CompiledPartitionRoute::HierarchicalV1,
            None,
            vec![Some(1), Some(2)],
        );
        let state = RuntimeState::default();
        let original = ExecutionFailure::from(novarocks_functions::KernelFailure::Operational(
            novarocks_functions::KernelDiagnostic::new("network partition original failure"),
        ));
        let control = state.error_state();
        control.set_error(original.clone());
        let mut work =
            crate::exec::pipeline::step_context::TurnWork::new(std::num::NonZeroU64::MIN, 64);
        let mut conversions = novarocks_value_arrays::decode::ValueConversionContext::default();
        let mut ctx = StepContext::new(&control, &mut work, &mut conversions);
        assert_eq!(
            instance_partition_rows(&batch, &source, 2, 3, &mut 0, &mut ctx).unwrap_err(),
            original
        );
    }

    #[test]
    fn async_send_failure_remains_visible_while_task_conclusion_owner_is_deferred() {
        use crate::runtime::fragment::io::{
            ExchangeTransmitRejection, FragmentIoError, FragmentIoErrorKind, FragmentIoOperation,
        };
        use crate::runtime::task_control::test_support::DeferredOwner;
        use novarocks_execution_contract::{TaskConclusionPort, TaskFailurePhase};
        struct FailedTransmit;
        impl ExchangeFrameTransmitter for FailedTransmit {
            fn transmit(&self, _: ExchangeFrame) -> Result<(), ExchangeTransmitRejection> {
                Err(ExchangeTransmitRejection::Failed(FragmentIoError::new(
                    FragmentIoOperation::ExchangeTransmit,
                    FragmentIoErrorKind::Unavailable,
                    "actual receiver transport failure",
                )))
            }
        }
        let owner = DeferredOwner::new();
        let state = Arc::new(RuntimeErrorState::for_task(Arc::new(
            owner.view(TaskFailurePhase::Execution),
        )));
        let mut sink = make_test_operator();
        sink.error_state = Some(state.clone());
        sink.end_of_stream_sent.store(true, Ordering::Release);
        sink.finishing.store(true, Ordering::Release);
        let mut task = make_test_exchange_send_task();
        task.error_state = state.clone();
        task.tracker = sink.send_tracker.clone();
        task.transmitter = Arc::new(FailedTransmit);
        sink.exchange_queue().try_submit(task, false).unwrap();
        let until = Instant::now() + Duration::from_secs(2);
        while !sink.send_tracker.is_idle() && Instant::now() < until {
            std::thread::yield_now();
        }
        assert!(sink.send_tracker.is_idle());
        assert!(owner.accepted().is_none());
        assert!(state.error().is_none());
        let original = sink
            .send_tracker
            .failure()
            .expect("actual I/O result is retained before proposal acceptance");
        assert!(sink.current_error().unwrap().same_occurrence(&original));
        assert!(!sink.maybe_mark_finished());
        owner.advance_all();
        assert!(state.error().unwrap().same_occurrence(&original));
        assert!(!sink.maybe_mark_finished());
    }

    #[test]
    fn required_withdrawal_cannot_skip_parked_payload_or_end_even_after_earlier_poll() {
        for pending in [false, true] {
            let mut sink = make_test_operator();
            sink.input.delivery =
                novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain;
            let destination = FragmentDestination::new(
                UniqueId::new(701, 702),
                RuntimeEndpoint::new("127.0.0.1", 9030).unwrap(),
                sink.fragment_instance_id,
                0,
                1,
            )
            .unwrap();
            gate_destinations(&mut sink, vec![destination]);
            let gates = sink.edge_gates.as_ref().unwrap().clone();
            sink.ensure_pending_buffers_initialized();
            if pending {
                sink.pending_payloads_per_dest[0] = Some(test_payload(3));
            }
            let gate = gates
                .gate_for_destination(ExchangeDestinationKey::new(
                    UniqueId::new(701, 702),
                    sink.input.dest_node_id,
                ))
                .unwrap();
            gate.close_for_normal_cancellation();
            assert!(
                if pending {
                    sink.flush_pending(true, false)
                } else {
                    sink.send_eos()
                }
                .is_err()
            );
            assert!(!sink.end_of_stream_sent.load(Ordering::Acquire));
            if pending {
                assert!(sink.pending_payloads_per_dest[0].is_some());
            }
        }
    }
}

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
        let original = crate::runtime::fragment::ExecutionFailure::from(
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
