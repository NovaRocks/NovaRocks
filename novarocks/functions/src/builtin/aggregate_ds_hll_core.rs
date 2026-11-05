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

//! Original DS HLL aggregate computation over explicit state/allocator access.
use crate::aggregate_scalar::ScalarStateAllocator;
use crate::builtin::aggregate_ds_hll_failure::*;
use crate::datasketches_hll::{HllAccumulator, HllTargetType};
use crate::datasketches_hll_failure::HllObservation;
use crate::percentile_input::payload_bytes_at_with_failure;
use crate::sketch_hash::prehash_array_value_with_failure;
use allocator_api2::vec::Vec as AllocVec;
use arrow_array::builder::{ArrayBuilder, BinaryBuilder, Int64Builder};
use arrow_array::{Array, ArrayRef, BinaryArray, LargeStringArray, StringArray, StructArray};
use arrow_schema::DataType;
use std::sync::Arc;

pub const DEFAULT_LOG_K: u8 = 17;
pub const DEFAULT_TARGET_TYPE: HllTargetType = HllTargetType::Hll6;
#[derive(Clone, Copy, Debug)]
pub enum DsHllUpdateMode {
    Hash,
    Merge,
    Count,
}
/// The host owns retained charge, actual admission, state and cleanup.
pub trait DsHllStorage {
    type Allocator: ScalarStateAllocator;
    fn allocator(&self) -> Self::Allocator;
    fn accumulator(&self) -> Option<&HllAccumulator>;
    fn ensure_accumulator(
        &mut self,
        log_k: u8,
        target: HllTargetType,
    ) -> Result<(), DsHllAggregateFailure>;
    fn update_hash(&mut self, hash: u64) -> Result<(), DsHllAggregateFailure>;
    fn merge_payload(&mut self, payload: &[u8]) -> Result<(), DsHllAggregateFailure>;
}
/// This is an ordinal-to-state borrowing port, not a private aggregate arena.
pub trait DsHllStateAccess {
    type State: DsHllStorage;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn with_state<T>(
        &mut self,
        ordinal: usize,
        visit: impl FnOnce(&mut Self::State) -> Result<T, DsHllAggregateFailure>,
    ) -> Result<T, DsHllAggregateFailure>;
}
pub trait DsHllStateReadAccess {
    type State: DsHllStorage;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn state(&self, ordinal: usize) -> &Self::State;
}

/// Actual ordinal access is supplied by the caller. It never synthesizes
/// selected row zero, a global row, or a private aggregate arena.
pub trait TypedDsHllStorage<F: DsHllFailureSink> {
    type Allocator: ScalarStateAllocator;
    fn allocator(&self) -> Self::Allocator;
    fn accumulator(&self) -> Option<&HllAccumulator>;
    fn ensure_accumulator(
        &mut self,
        log_k: u8,
        target: HllTargetType,
        sink: &mut F,
    ) -> Result<(), F::Error>;
    fn update_hash(&mut self, hash: u64, sink: &mut F) -> Result<(), F::Error>;
    fn merge_payload(&mut self, payload: &[u8], sink: &mut F) -> Result<(), F::Error>;
}
impl<T: DsHllStorage> TypedDsHllStorage<RawDsHllFailure> for T {
    type Allocator = T::Allocator;
    fn allocator(&self) -> Self::Allocator {
        DsHllStorage::allocator(self)
    }
    fn accumulator(&self) -> Option<&HllAccumulator> {
        DsHllStorage::accumulator(self)
    }
    fn ensure_accumulator(
        &mut self,
        log_k: u8,
        target: HllTargetType,
        _: &mut RawDsHllFailure,
    ) -> Result<(), DsHllAggregateFailure> {
        DsHllStorage::ensure_accumulator(self, log_k, target)
    }
    fn update_hash(
        &mut self,
        hash: u64,
        _: &mut RawDsHllFailure,
    ) -> Result<(), DsHllAggregateFailure> {
        DsHllStorage::update_hash(self, hash)
    }
    fn merge_payload(
        &mut self,
        payload: &[u8],
        _: &mut RawDsHllFailure,
    ) -> Result<(), DsHllAggregateFailure> {
        DsHllStorage::merge_payload(self, payload)
    }
}
pub trait TypedDsHllStateAccess<F: DsHllFailureSink> {
    type State: TypedDsHllStorage<F>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn source_row(&self, ordinal: usize) -> usize;
    fn with_state<T>(
        &mut self,
        ordinal: usize,
        visit: impl FnOnce(&mut Self::State) -> Result<T, F::Error>,
    ) -> Result<T, F::Error>;
}
impl<T: DsHllStateAccess> TypedDsHllStateAccess<RawDsHllFailure> for T {
    type State = T::State;
    fn len(&self) -> usize {
        DsHllStateAccess::len(self)
    }
    fn source_row(&self, ordinal: usize) -> usize {
        ordinal
    }
    fn with_state<V>(
        &mut self,
        ordinal: usize,
        visit: impl FnOnce(&mut Self::State) -> Result<V, DsHllAggregateFailure>,
    ) -> Result<V, DsHllAggregateFailure> {
        DsHllStateAccess::with_state(self, ordinal, visit)
    }
}
fn parse_target_type_with_sink<F: DsHllFailureSink>(
    value: &str,
    sink: &mut F,
) -> Result<HllTargetType, F::Error> {
    let _reservation = sink.reserve_temporary(value.len())?;
    if !value.is_empty() {
        sink.observe(HllObservation::OpaqueBoundary)?;
    }
    let normalized = value.to_ascii_uppercase();
    let result = match normalized.as_str() {
        "HLL_4" => HllTargetType::Hll4,
        "HLL_8" => HllTargetType::Hll8,
        _ => HllTargetType::Hll6,
    };
    drop(normalized);
    if !value.is_empty() {
        sink.observe(HllObservation::OpaqueBoundary)?;
    }
    Ok(result)
}

fn parse_log_k_with_sink<F: DsHllFailureSink>(
    array: &ArrayRef,
    row: usize,
    context: DsHllInputContext,
    sink: &mut F,
) -> Result<Option<u8>, F::Error> {
    match array.data_type() {
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow_array::Int8Array>()
                .ok_or_else(|| {
                    sink.input(DsHllInputRecipe {
                        context,
                        detail: DsHllInputDetail::Downcast("Int8Array"),
                    })
                })?;
            Ok((!arr.is_null(row)).then_some(arr.value(row) as u8))
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow_array::Int16Array>()
                .ok_or_else(|| {
                    sink.input(DsHllInputRecipe {
                        context,
                        detail: DsHllInputDetail::Downcast("Int16Array"),
                    })
                })?;
            Ok((!arr.is_null(row)).then_some(arr.value(row) as u8))
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow_array::Int32Array>()
                .ok_or_else(|| {
                    sink.input(DsHllInputRecipe {
                        context,
                        detail: DsHllInputDetail::Downcast("Int32Array"),
                    })
                })?;
            Ok((!arr.is_null(row)).then_some(arr.value(row) as u8))
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .ok_or_else(|| {
                    sink.input(DsHllInputRecipe {
                        context,
                        detail: DsHllInputDetail::Downcast("Int64Array"),
                    })
                })?;
            Ok((!arr.is_null(row)).then_some(arr.value(row) as u8))
        }
        other => Err(sink.input(DsHllInputRecipe {
            context,
            detail: DsHllInputDetail::LogK(other),
        })),
    }
}

fn parse_target_type_array_with_sink<F: DsHllFailureSink>(
    array: &ArrayRef,
    row: usize,
    context: DsHllInputContext,
    sink: &mut F,
) -> Result<Option<HllTargetType>, F::Error> {
    match array.data_type() {
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .ok_or_else(|| {
                    sink.input(DsHllInputRecipe {
                        context,
                        detail: DsHllInputDetail::Downcast("StringArray"),
                    })
                })?;
            let target = parse_target_type_with_sink(arr.value(row), sink)?;
            Ok((!arr.is_null(row)).then_some(target))
        }
        DataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow_array::LargeStringArray>()
                .ok_or_else(|| {
                    sink.input(DsHllInputRecipe {
                        context,
                        detail: DsHllInputDetail::Downcast("LargeStringArray"),
                    })
                })?;
            let target = parse_target_type_with_sink(arr.value(row), sink)?;
            Ok((!arr.is_null(row)).then_some(target))
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    sink.input(DsHllInputRecipe {
                        context,
                        detail: DsHllInputDetail::Downcast("BinaryArray"),
                    })
                })?;
            let target = parse_target_type_with_sink(
                std::str::from_utf8(arr.value(row)).unwrap_or_default(),
                sink,
            )?;
            Ok((!arr.is_null(row)).then_some(target))
        }
        DataType::Null => Ok(None),
        other => Err(sink.input(DsHllInputRecipe {
            context,
            detail: DsHllInputDetail::Target(other),
        })),
    }
}

fn update_from_struct_with_sink<F: DsHllFailureSink, S: TypedDsHllStateAccess<F>>(
    array: &StructArray,
    access: &mut S,
    context: DsHllInputContext,
    sink: &mut F,
) -> Result<(), F::Error> {
    let fields = array.columns();
    if fields.is_empty() {
        return Err(sink.input(DsHllInputRecipe {
            context,
            detail: DsHllInputDetail::EmptyStruct,
        }));
    }
    let values = fields[0].clone();

    for ordinal in 0..access.len() {
        sink.observe(HllObservation::Step)?;
        let row = access.source_row(ordinal);
        update_hash_channels_at(
            DsHllChannel {
                array: &values,
                row,
            },
            fields.get(1).map(|array| DsHllChannel { array, row }),
            fields.get(2).map(|array| DsHllChannel { array, row }),
            access,
            ordinal,
            context,
            sink,
        )?;
    }
    Ok(())
}

/// Actual evaluated source/address facts, never guessed channel defaults.
#[derive(Clone, Copy)]
pub struct DsHllChannel<'a> {
    pub array: &'a ArrayRef,
    pub row: usize,
}
/// This is the one original packed-Struct row body. Both the original
/// packed carrier and selected logical channels borrow it directly.
pub fn update_hash_channels_at<F: DsHllFailureSink, S: TypedDsHllStateAccess<F>>(
    values: DsHllChannel<'_>,
    log_k: Option<DsHllChannel<'_>>,
    target: Option<DsHllChannel<'_>>,
    access: &mut S,
    ordinal: usize,
    context: DsHllInputContext,
    sink: &mut F,
) -> Result<(), F::Error> {
    let lg_k = match log_k {
        Some(channel) => parse_log_k_with_sink(channel.array, channel.row, context, sink)?
            .unwrap_or(DEFAULT_LOG_K),
        None => DEFAULT_LOG_K,
    };
    let target_type = match target {
        Some(channel) => {
            parse_target_type_array_with_sink(channel.array, channel.row, context, sink)?
                .unwrap_or(DEFAULT_TARGET_TYPE)
        }
        None => DEFAULT_TARGET_TYPE,
    };
    let Some(hash) = prehash_array_value_with_failure(
        values.array,
        values.row,
        &mut HashSink { sink, context },
    )?
    else {
        return Ok(());
    };
    access.with_state(ordinal, |state| {
        state.ensure_accumulator(lg_k, target_type, sink)?;
        state.update_hash(hash, sink)
    })
}

fn update_from_raw_array_with_sink<F: DsHllFailureSink, S: TypedDsHllStateAccess<F>>(
    array: &ArrayRef,
    access: &mut S,
    log_k: u8,
    target_type: HllTargetType,
    context: DsHllInputContext,
    sink: &mut F,
) -> Result<(), F::Error> {
    for ordinal in 0..access.len() {
        sink.observe(HllObservation::Step)?;
        let row = access.source_row(ordinal);
        let Some(hash) =
            prehash_array_value_with_failure(array, row, &mut HashSink { sink, context })?
        else {
            continue;
        };
        access.with_state(ordinal, |state| {
            state.ensure_accumulator(log_k, target_type, sink)?;
            state.update_hash(hash, sink)
        })?;
    }
    Ok(())
}

pub fn merge_batch<S: DsHllStateAccess>(
    array: &ArrayRef,
    access: &mut S,
    _context: &str,
) -> Result<(), DsHllAggregateFailure> {
    merge_batch_with_sink(
        array,
        access,
        DsHllInputContext::Merge,
        &mut RawDsHllFailure,
    )
}

pub fn merge_batch_with_sink<F: DsHllFailureSink, S: TypedDsHllStateAccess<F>>(
    array: &ArrayRef,
    access: &mut S,
    context: DsHllInputContext,
    sink: &mut F,
) -> Result<(), F::Error> {
    for ordinal in 0..access.len() {
        sink.observe(HllObservation::Step)?;
        let row = access.source_row(ordinal);
        access.with_state(ordinal, |state| {
            let Some(payload) =
                payload_bytes_for_merge_with_sink(array, row, context, state.allocator(), sink)?
            else {
                return Ok(());
            };
            state.merge_payload(payload.as_ref(), sink)
        })?;
    }
    Ok(())
}

enum MergePayload<'a, A: ScalarStateAllocator> {
    Borrowed(&'a [u8]),
    Owned(AllocVec<u8, A>),
}

impl<A: ScalarStateAllocator> AsRef<[u8]> for MergePayload<'_, A> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Borrowed(value) => value,
            Self::Owned(value) => value.as_slice(),
        }
    }
}

fn payload_bytes_for_merge_with_sink<'a, A: ScalarStateAllocator, F: DsHllFailureSink>(
    array: &'a ArrayRef,
    row: usize,
    context: DsHllInputContext,
    allocator: A,
    sink: &mut F,
) -> Result<Option<MergePayload<'a, A>>, F::Error> {
    match array.data_type() {
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    sink.input(DsHllInputRecipe {
                        context,
                        detail: DsHllInputDetail::Downcast("StringArray"),
                    })
                })?;
            if arr.is_null(row) {
                return Ok(None);
            }
            let value = arr.value(row);
            let mut payload = AllocVec::new_in(allocator.clone());
            if !value.is_empty() {
                sink.observe(HllObservation::OpaqueBoundary)?;
            }
            payload.try_reserve_exact(value.len()).map_err(|source| {
                sink.kernel(
                    allocator.scalar_allocation_error("reserve ds_hll string payload", source),
                )
            })?;
            if !value.is_empty() {
                sink.observe(HllObservation::OpaqueBoundary)?;
            }
            for ch in value.chars() {
                payload.push(ch as u8);
                sink.observe(HllObservation::Step)?;
            }
            Ok(Some(MergePayload::Owned(payload)))
        }
        DataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| {
                    sink.input(DsHllInputRecipe {
                        context,
                        detail: DsHllInputDetail::Downcast("LargeStringArray"),
                    })
                })?;
            if arr.is_null(row) {
                return Ok(None);
            }
            let value = arr.value(row);
            let mut payload = AllocVec::new_in(allocator.clone());
            if !value.is_empty() {
                sink.observe(HllObservation::OpaqueBoundary)?;
            }
            payload.try_reserve_exact(value.len()).map_err(|source| {
                sink.kernel(
                    allocator.scalar_allocation_error("reserve ds_hll string payload", source),
                )
            })?;
            if !value.is_empty() {
                sink.observe(HllObservation::OpaqueBoundary)?;
            }
            for ch in value.chars() {
                payload.push(ch as u8);
                sink.observe(HllObservation::Step)?;
            }
            Ok(Some(MergePayload::Owned(payload)))
        }
        _ => payload_bytes_at_with_failure(array, row, &mut |failure| {
            sink.input(DsHllInputRecipe {
                context,
                detail: DsHllInputDetail::Payload(failure),
            })
        })
        .map(|payload| payload.map(MergePayload::Borrowed)),
    }
}

/// The sole original Struct test; even an unknown legacy kind must visit it first.
pub fn update_struct_if_present<S: DsHllStateAccess>(
    array: &ArrayRef,
    access: &mut S,
) -> Result<Option<()>, DsHllAggregateFailure> {
    update_struct_if_present_with_sink(array, access, &mut RawDsHllFailure)
}

pub fn update_struct_if_present_with_sink<F: DsHllFailureSink, S: TypedDsHllStateAccess<F>>(
    array: &ArrayRef,
    access: &mut S,
    sink: &mut F,
) -> Result<Option<()>, F::Error> {
    if let Some(struct_array) = array.as_any().downcast_ref::<StructArray>() {
        update_from_struct_with_sink(struct_array, access, DsHllInputContext::CountDistinct, sink)?;
        Ok(Some(()))
    } else {
        Ok(None)
    }
}
pub fn update_batch<S: DsHllStateAccess>(
    mode: DsHllUpdateMode,
    array: &ArrayRef,
    access: &mut S,
) -> Result<(), DsHllAggregateFailure> {
    update_batch_with_sink(mode, array, access, &mut RawDsHllFailure)
}

pub fn update_batch_with_sink<F: DsHllFailureSink, S: TypedDsHllStateAccess<F>>(
    mode: DsHllUpdateMode,
    array: &ArrayRef,
    access: &mut S,
    sink: &mut F,
) -> Result<(), F::Error> {
    if update_struct_if_present_with_sink(array, access, sink)?.is_some() {
        return Ok(());
    }
    update_nonstruct_batch_with_sink(mode, array, access, sink)
}
pub fn update_nonstruct_batch<S: DsHllStateAccess>(
    mode: DsHllUpdateMode,
    array: &ArrayRef,
    access: &mut S,
) -> Result<(), DsHllAggregateFailure> {
    update_nonstruct_batch_with_sink(mode, array, access, &mut RawDsHllFailure)
}

pub fn update_nonstruct_batch_with_sink<F: DsHllFailureSink, S: TypedDsHllStateAccess<F>>(
    mode: DsHllUpdateMode,
    array: &ArrayRef,
    access: &mut S,
    sink: &mut F,
) -> Result<(), F::Error> {
    match mode {
        DsHllUpdateMode::Hash => update_from_raw_array_with_sink(
            array,
            access,
            DEFAULT_LOG_K,
            DEFAULT_TARGET_TYPE,
            DsHllInputContext::CountDistinct,
            sink,
        ),
        DsHllUpdateMode::Merge => {
            merge_batch_with_sink(array, access, DsHllInputContext::Merge, sink)
        }
        DsHllUpdateMode::Count => {
            merge_batch_with_sink(array, access, DsHllInputContext::CountDistinct, sink)
        }
    }
}

/// Payload ownership includes the actual opaque reservation in the pure
/// host adapter, while the raw adapter retains its original Vec owner.
pub trait DsHllEmissionPort<F: DsHllFailureSink> {
    type Payload: AsRef<[u8]>;
    fn empty_payload(&mut self, sink: &mut F) -> Result<Self::Payload, F::Error>;
    fn serialize(
        &mut self,
        accumulator: &HllAccumulator,
        sink: &mut F,
    ) -> Result<Self::Payload, F::Error>;
    fn clone_empty(
        &mut self,
        empty: &Self::Payload,
        sink: &mut F,
    ) -> Result<Self::Payload, F::Error>;
}
pub trait DsHllEmissionStates {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn accumulator(&self, ordinal: usize) -> Option<&HllAccumulator>;
}
impl<S: DsHllStateReadAccess> DsHllEmissionStates for S {
    fn len(&self) -> usize {
        DsHllStateReadAccess::len(self)
    }
    fn accumulator(&self, ordinal: usize) -> Option<&HllAccumulator> {
        DsHllStorage::accumulator(self.state(ordinal))
    }
}
struct RawEmission;
impl DsHllEmissionPort<RawDsHllFailure> for RawEmission {
    type Payload = Vec<u8>;
    fn empty_payload(&mut self, _: &mut RawDsHllFailure) -> Result<Vec<u8>, DsHllAggregateFailure> {
        Ok(HllAccumulator::new_unreserved(DEFAULT_LOG_K, DEFAULT_TARGET_TYPE)?.serialize()?)
    }
    fn serialize(
        &mut self,
        accumulator: &HllAccumulator,
        _: &mut RawDsHllFailure,
    ) -> Result<Vec<u8>, DsHllAggregateFailure> {
        Ok(accumulator.serialize()?)
    }
    fn clone_empty(
        &mut self,
        empty: &Vec<u8>,
        _: &mut RawDsHllFailure,
    ) -> Result<Vec<u8>, DsHllAggregateFailure> {
        Ok(empty.clone())
    }
}
pub fn build_array<S: DsHllStateReadAccess>(
    output_type: &DataType,
    access: &S,
) -> Result<ArrayRef, DsHllAggregateFailure> {
    build_array_with_sink(output_type, access, &mut RawEmission, &mut RawDsHllFailure)
}
/// The one original Binary/Int64 builder loop. This does not authorize its
/// Arrow allocations: the actual invocation host still owns that scope.
pub fn build_array_with_sink<
    F: DsHllFailureSink,
    S: DsHllEmissionStates,
    P: DsHllEmissionPort<F>,
>(
    output_type: &DataType,
    access: &S,
    port: &mut P,
    sink: &mut F,
) -> Result<ArrayRef, F::Error> {
    match output_type {
        DataType::Binary => {
            sink.observe(HllObservation::OpaqueBoundary)?;
            let mut builder = BinaryBuilder::new();
            sink.observe(HllObservation::OpaqueBoundary)?;
            let empty_payload = port.empty_payload(sink)?;
            let mut extent = DsHllOutputTally::default();
            for ordinal in 0..access.len() {
                sink.observe(HllObservation::Step)?;
                let payload = match access.accumulator(ordinal) {
                    Some(accumulator) => port.serialize(accumulator, sink)?,
                    None => port.clone_empty(&empty_payload, sink)?,
                };
                extent
                    .append(payload.as_ref().len())
                    .map_err(|cause| sink.kernel(cause))?;
                sink.observe(HllObservation::OpaqueBoundary)?;
                builder.append_value(payload.as_ref());
                drop(payload);
                sink.observe(HllObservation::OpaqueBoundary)?;
            }
            sink.observe(HllObservation::OpaqueBoundary)?;
            let result = Arc::new(builder.finish()) as ArrayRef;
            sink.observe(HllObservation::OpaqueBoundary)?;
            Ok(result)
        }
        DataType::Int64 => {
            sink.observe(HllObservation::OpaqueBoundary)?;
            let mut builder = Int64Builder::new();
            sink.observe(HllObservation::OpaqueBoundary)?;
            for ordinal in 0..access.len() {
                sink.observe(HllObservation::Step)?;
                let value = access
                    .accumulator(ordinal)
                    .map(|accumulator| accumulator.estimate_with_failure(sink))
                    .transpose()?
                    .unwrap_or(0);
                let grows = builder.len() == builder.capacity();
                if grows {
                    sink.observe(HllObservation::OpaqueBoundary)?;
                }
                builder.append_value(value);
                if grows {
                    sink.observe(HllObservation::OpaqueBoundary)?;
                }
            }
            sink.observe(HllObservation::OpaqueBoundary)?;
            let result = Arc::new(builder.finish()) as ArrayRef;
            sink.observe(HllObservation::OpaqueBoundary)?;
            Ok(result)
        }
        other => Err(sink.input(DsHllInputRecipe {
            context: DsHllInputContext::CountDistinct,
            detail: DsHllInputDetail::Output(other),
        })),
    }
}
