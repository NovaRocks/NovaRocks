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

//! Exact Utf8 and Binary byte extrema; host memory scopes own replacement coexistence.

use super::aggregate_extrema::ExtremaOperation;
use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::*;
use arrow_array::{
    Array, ArrayRef, BinaryArray, GenericByteArray, StringArray,
    types::{ByteArrayType, GenericBinaryType, GenericStringType},
};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer};
use arrow_schema::DataType;
use std::{alloc::Layout, cmp::Ordering, ops::Range, sync::Arc};

/// Inline state belongs to the host arena; only this Vec's actual capacity is
/// additional retained heap. None and Some(empty) are distinct SQL values.
#[derive(Debug)]
pub(super) struct BytesExtremaState {
    value: Option<Vec<u8>>,
}
impl BytesExtremaState {
    pub(super) fn retained_bytes(&self) -> usize {
        self.value.as_ref().map_or(0, Vec::capacity)
    }
}
#[derive(Debug)]
pub(super) struct BytesExtremaKernel {
    pub(super) contract: Arc<AggregateCallContract>,
    pub(super) operation: ExtremaOperation,
}
fn maximum_retained() -> usize {
    (i32::MAX as usize).min(isize::MAX as usize)
}
fn observed<T>(
    control: &dyn KernelEvaluationControl,
    body: impl FnOnce(&mut EvaluationCheckpoints<'_>) -> Result<T, KernelFailure>,
) -> Result<T, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = body(&mut work);
    if matches!(&result, Err(cause) if cause.is_interruption()) {
        return result;
    }
    work.finish()?;
    result
}
fn copy_error(error: crate::selected_copy::CopyError) -> KernelFailure {
    error.into_kernel()
}
fn reserve<T>(count: usize, work: &mut EvaluationCheckpoints<'_>) -> Result<Vec<T>, KernelFailure> {
    Layout::array::<T>(count).map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    Ok(values)
}
struct SelectedBytes<'a> {
    array: &'a dyn Array,
    row: usize,
    value: &'a [u8],
}

fn read_array<'a, T: ByteArrayType<Offset = i32>>(
    argument: EvaluatedArgument<'a>,
    ordinal: usize,
    row: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<SelectedBytes<'a>>, KernelFailure> {
    let array = argument
        .array()
        .as_any()
        .downcast_ref::<GenericByteArray<T>>();
    work.step()?;
    let array = array.ok_or_else(|| internal("MIN/MAX has a foreign concrete byte carrier"))?;
    let at = argument.value_row(ordinal, row);
    let valid = at < array.len();
    work.step()?;
    if !valid {
        return Err(internal(
            "MIN/MAX source address is outside its byte carrier",
        ));
    }
    let null = array.is_null(at);
    work.step()?;
    Ok((!null).then(|| SelectedBytes {
        array,
        row: at,
        value: array.value(at).as_ref(),
    }))
}
fn compare(
    candidate: &[u8],
    current: &[u8],
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Ordering, KernelFailure> {
    for (a, b) in candidate.iter().zip(current) {
        let order = a.cmp(b);
        work.step()?;
        if order != Ordering::Equal {
            return Ok(order);
        }
    }
    let order = candidate.len().cmp(&current.len());
    work.step()?;
    Ok(order)
}
fn replacement(
    state: &BytesExtremaState,
    source: Option<SelectedBytes<'_>>,
    operation: ExtremaOperation,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<Vec<u8>>, KernelFailure> {
    let Some(SelectedBytes {
        array,
        row: at,
        value: bytes,
    }) = source
    else {
        return Ok(None);
    };
    let replace = if let Some(current) = &state.value {
        compare(bytes, current, work)?
            == match operation {
                ExtremaOperation::Min => Ordering::Less,
                ExtremaOperation::Max => Ordering::Greater,
            }
    } else {
        work.step()?;
        true
    };
    if !replace {
        return Ok(None);
    }
    // The sole selected copy author checks this exact original source address.
    // Its Range/Block/to_data scratch is delegated and opaque, not a MEM grant.
    Layout::array::<Range<usize>>(1).map_err(|_| KernelFailure::ResourceExhausted)?;
    let address = u64::try_from(at).map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    crate::selected_copy::preflight_take(array, &[Some(address)], |opaque| {
        if opaque { work.flush() } else { work.step() }
    })
    .map_err(copy_error)?;
    crate::selected_copy::byte_interleave_payload_extent(bytes.len(), false).map_err(copy_error)?;
    if bytes.len() > maximum_retained() {
        return Err(KernelFailure::ResourceExhausted);
    }
    let mut replacement = reserve::<u8>(bytes.len(), work)?;
    // A successful allocator may report excess capacity. The retained promise
    // concerns the actual capacity, never only the requested byte count.
    if replacement.capacity() > maximum_retained() {
        return Err(KernelFailure::ResourceExhausted);
    }
    for byte in bytes {
        replacement.push(*byte);
        work.step()?;
    }
    Ok(Some(replacement))
}

impl BytesExtremaKernel {
    fn carrier(&self) -> &DataType {
        self.contract.intermediate_type().data_type()
    }

    fn accepts(&self, array: &dyn Array) -> bool {
        match self.carrier() {
            DataType::Utf8 => array.as_any().is::<StringArray>(),
            DataType::Binary => array.as_any().is::<BinaryArray>(),
            _ => false,
        }
    }

    fn read<'a>(
        &self,
        argument: EvaluatedArgument<'a>,
        ordinal: usize,
        row: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Option<SelectedBytes<'a>>, KernelFailure> {
        match self.carrier() {
            DataType::Utf8 => read_array::<GenericStringType<i32>>(argument, ordinal, row, work),
            DataType::Binary => read_array::<GenericBinaryType<i32>>(argument, ordinal, row, work),
            _ => Err(internal("MIN/MAX has an unsupported byte carrier")),
        }
    }

    fn build<'a>(
        &self,
        states: impl ExactSizeIterator<Item = &'a BytesExtremaState>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure> {
        match self.carrier() {
            DataType::Utf8 => build_array::<GenericStringType<i32>>(states, control),
            DataType::Binary => build_array::<GenericBinaryType<i32>>(states, control),
            _ => Err(internal("MIN/MAX cannot emit its byte carrier")),
        }
    }
}

impl PreparedAggregateKernel for BytesExtremaKernel {
    fn supports_local_stages(&self) -> bool {
        true
    }
    fn clone_for_local_phase(
        &self,
        contract: Arc<AggregateCallContract>,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Arc<Self>, KernelFailure> {
        control
            .checkpoint(
                novarocks_type_contract::CompilePhase::FunctionSpecialization,
                0,
            )
            .map_err(crate::kernel_control::compile_failure)?;
        Ok(Arc::new(Self {
            contract,
            operation: self.operation,
        }))
    }
    type State = BytesExtremaState;
    type PreparedUpdateBatch<'batch> = SelectedAggregateUpdateInput<'batch, 'batch>;
    type PreparedMergeBatch<'batch> = SelectedAggregateMergeInput<'batch, 'batch>;
    fn contract(&self) -> &Arc<AggregateCallContract> {
        &self.contract
    }
    fn memory_policy(&self) -> AggregateStateMemoryPolicy {
        AggregateStateMemoryPolicy::BoundedRetained {
            max_retained_bytes_per_state: maximum_retained(),
        }
    }
    fn retained_bytes(&self, state: &Self::State) -> usize {
        state.retained_bytes()
    }
    fn create_state(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        observed(control, |work| {
            work.step()?;
            Ok(BytesExtremaState { value: None })
        })
    }
    fn prepare_update<'batch>(
        &'batch self,
        input: Self::PreparedUpdateBatch<'batch>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedUpdateBatch<'batch>, KernelFailure> {
        observed(control, |work| {
            let exact = std::ptr::eq(input.contract(), self.contract.as_ref())
                && self.contract.phase().consumes_logical_arguments()
                && input.logical_arguments().len() == 1
                && input.order_arguments().is_empty();
            work.step()?;
            if !exact {
                return Err(invalid(
                    "Byte MIN/MAX update differs from its exact contract or channels",
                ));
            }
            let concrete = self.accepts(input.logical_arguments()[0].array().as_ref());
            work.step()?;
            if !concrete {
                return Err(internal(
                    "Byte MIN/MAX update has a foreign concrete carrier",
                ));
            }
            Ok(input)
        })
    }
    fn update_row<'batch>(
        &self,
        state: &mut Self::State,
        input: &Self::PreparedUpdateBatch<'batch>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        let new = observed(control, |work| {
            let row = input.selection().row(ordinal);
            work.step()?;
            let row =
                row.ok_or_else(|| invalid("Byte MIN/MAX update ordinal is outside its selection"))?;
            replacement(
                state,
                self.read(input.logical_arguments()[0], ordinal, row, work)?,
                self.operation,
                work,
            )
        })?;
        // Only this leaf's successful own scope authorizes the swap. Outer erased
        // post-check refusal poisons its invocation; it is not a rollback claim.
        if let Some(value) = new {
            state.value = Some(value);
        }
        Ok(())
    }
    fn prepare_merge<'batch>(
        &'batch self,
        input: Self::PreparedMergeBatch<'batch>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedMergeBatch<'batch>, KernelFailure> {
        observed(control, |work| {
            let exact = std::ptr::eq(input.contract(), self.contract.as_ref())
                && !self.contract.phase().consumes_logical_arguments();
            work.step()?;
            if !exact {
                return Err(invalid(
                    "Byte MIN/MAX merge differs from its exact contract or phase",
                ));
            }
            let concrete = self.accepts(input.state().array().as_ref());
            work.step()?;
            if !concrete {
                return Err(internal(
                    "Byte MIN/MAX merge has a foreign concrete carrier",
                ));
            }
            Ok(input)
        })
    }
    fn merge_row<'batch>(
        &self,
        state: &mut Self::State,
        input: &Self::PreparedMergeBatch<'batch>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        let new = observed(control, |work| {
            let row = input.selection().row(ordinal);
            work.step()?;
            let row =
                row.ok_or_else(|| invalid("Byte MIN/MAX merge ordinal is outside its selection"))?;
            replacement(
                state,
                self.read(input.state(), ordinal, row, work)?,
                self.operation,
                work,
            )
        })?;
        if let Some(value) = new {
            state.value = Some(value);
        }
        Ok(())
    }
    fn build_intermediate<'state, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'state Self::State>,
    {
        self.build(states, control)
    }
    fn build_final<'state, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'state Self::State>,
    {
        self.build(states, control)
    }
}
fn build_array<'state, T: ByteArrayType<Offset = i32>>(
    states: impl ExactSizeIterator<Item = &'state BytesExtremaState>,
    control: &dyn KernelEvaluationControl,
) -> Result<ArrayRef, KernelFailure> {
    observed(control, |work| {
        let rows = states.len();
        crate::selected_copy::guarded_interleave_extent(&T::DATA_TYPE, rows).map_err(copy_error)?;
        let mut sources = reserve::<&BytesExtremaState>(rows, work)?;
        let mut bytes = 0usize;
        for state in states {
            let within = sources.len() < rows;
            work.step()?;
            if !within {
                return Err(internal(
                    "Byte MIN/MAX emission iterator exceeds its declared extent",
                ));
            }
            bytes = bytes
                .checked_add(state.value.as_ref().map_or(0, Vec::len))
                .ok_or(KernelFailure::ResourceExhausted)?;
            crate::selected_copy::byte_interleave_payload_extent(bytes, false)
                .map_err(copy_error)?;
            sources.push(state);
            work.step()?;
        }
        let exact = sources.len() == rows;
        work.step()?;
        if !exact {
            return Err(internal(
                "Byte MIN/MAX emission iterator differs from its declared extent",
            ));
        }
        let mut offsets = reserve::<i32>(
            rows.checked_add(1)
                .ok_or(KernelFailure::ResourceExhausted)?,
            work,
        )?;
        let bitmap_bytes = rows
            .checked_add(7)
            .ok_or(KernelFailure::ResourceExhausted)?
            / 8;
        let mut validity = reserve::<u8>(bitmap_bytes, work)?;
        for _ in 0..bitmap_bytes {
            validity.push(0);
            work.step()?;
        }
        let mut values = reserve::<u8>(bytes, work)?;
        offsets.push(0);
        for (row, source) in sources.into_iter().enumerate() {
            if let Some(value) = &source.value {
                validity[row / 8] |= 1 << (row % 8);
                for byte in value {
                    values.push(*byte);
                    work.step()?;
                }
            }
            offsets
                .push(i32::try_from(values.len()).map_err(|_| KernelFailure::ResourceExhausted)?);
            work.step()?;
        }
        work.flush()?;
        let nulls = NullBuffer::new(BooleanBuffer::new(Buffer::from(validity), 0, rows));
        let result = GenericByteArray::<T>::try_new(
            OffsetBuffer::new(offsets.into()),
            Buffer::from(values),
            Some(nulls),
        );
        work.flush()?;
        let result = result
            .map_err(|_| internal("MIN/MAX state emission violated its exact byte format"))?;
        work.flush()?;
        let array = Arc::new(result) as ArrayRef;
        work.flush()?;
        Ok(array)
    })
}
#[cfg(test)]
#[path = "aggregate_extrema_utf8_tests.rs"]
mod tests;
