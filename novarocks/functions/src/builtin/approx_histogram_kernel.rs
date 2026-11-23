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

use super::{
    aggregate_map_core::parse_map_type, approx_distinct_kernel::observed,
    approx_percentile_digest::Digest,
};
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_scalar::{
    ScalarWork, TrackedAggScalarValue, aggregate_vec_with_capacity, checked_map_output_offset,
    tracked_scalar_from_array,
};
use crate::approx_top_k_core::{ApproxTopKState, update_one_observed};
use crate::kernel_control::{invalid, invalid_function_argument};
use crate::{
    AggregateCallContract, AggregateKernelPhase, AggregateStateAllocator,
    AggregateStateMemoryPolicy, KernelEvaluationControl, KernelFailure, PreparedAggregateKernel,
    SelectedAggregateMergeInput, SelectedAggregateUpdateInput,
};
use allocator_api2::vec::Vec as StateVec;
use arrow_array::{
    Array, ArrayRef, ArrowPrimitiveType, Float32Array, Float64Array, Int64Array, ListArray,
    MapArray, StructArray,
    builder::{Int64Builder, ListBuilder, PrimitiveBuilder, StringBuilder},
    types::{Float32Type, Float64Type, Int64Type},
};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_schema::DataType;
use std::sync::Arc;

#[derive(Debug, Clone, Copy)]
enum NumericInput {
    Bigint,
    Real,
    Double,
}

impl NumericInput {
    fn sample(self, array: &dyn Array, row: usize) -> Result<f64, KernelFailure> {
        let value = match self {
            Self::Bigint => {
                let value = at::<Int64Array>(array)?.value(row);
                let converted = value as f64;
                if converted as i64 != value {
                    return Err(invalid_function_argument(
                        "approx_percentile BIGINT value cannot be represented exactly as DOUBLE",
                    ));
                }
                converted
            }
            Self::Real => f64::from(at::<Float32Array>(array)?.value(row)),
            Self::Double => at::<Float64Array>(array)?.value(row),
        };
        if !value.is_finite() {
            return Err(invalid_function_argument(
                "approx_percentile value must be finite",
            ));
        }
        Ok(value)
    }
}

#[derive(Clone, Copy, Debug)]
struct PercentileProfile {
    numeric: NumericInput,
    weighted: bool,
    array: bool,
}

#[derive(Debug)]
enum Profile {
    Percentile(PercentileProfile),
    Frequency,
}

enum Targets {
    Scalar(f64),
    Array(StateVec<f64, HostAggregateAllocator>),
}
struct PercentileState {
    digest: Digest<HostAggregateAllocator>,
    targets: Option<Targets>,
}

enum State {
    Percentile(PercentileState),
    Frequency(ApproxTopKState<HostAggregateAllocator>),
    Failed,
}

pub(crate) struct ApproxHistogramState {
    allocator: HostAggregateAllocator,
    value: State,
}

#[derive(Debug)]
pub(crate) struct ApproxHistogramKernel {
    contract: Arc<AggregateCallContract>,
    profile: Profile,
}

impl ApproxHistogramKernel {
    pub(super) fn percentile(contract: Arc<AggregateCallContract>) -> Result<Self, KernelFailure> {
        let mut values = contract.logical_argument_types();
        let numeric = match values.next().map(crate::FunctionValueType::data_type) {
            Some(DataType::Int64) => NumericInput::Bigint,
            Some(DataType::Float32) => NumericInput::Real,
            Some(DataType::Float64) => NumericInput::Double,
            _ => return Err(invalid("approx_percentile has an uninstalled sample type")),
        };
        let weighted = values.len() == 2;
        let array = matches!(
            values.last().map(crate::FunctionValueType::data_type),
            Some(DataType::List(_))
        );
        Self::new(
            contract,
            Profile::Percentile(PercentileProfile {
                numeric,
                weighted,
                array,
            }),
        )
    }

    pub(super) fn frequency(contract: Arc<AggregateCallContract>) -> Result<Self, KernelFailure> {
        Self::new(contract, Profile::Frequency)
    }

    fn new(contract: Arc<AggregateCallContract>, profile: Profile) -> Result<Self, KernelFailure> {
        if contract.phase() != AggregateKernelPhase::Single || contract.distinct() {
            return Err(invalid(
                "approximate histogram requires its installed Single phase without DISTINCT",
            ));
        }
        Ok(Self { contract, profile })
    }
}

impl PreparedAggregateKernel for ApproxHistogramKernel {
    type State = ApproxHistogramState;
    type PreparedUpdateBatch<'a> = SelectedAggregateUpdateInput<'a, 'a>;
    type PreparedMergeBatch<'a> = ();

    fn contract(&self) -> &Arc<AggregateCallContract> {
        &self.contract
    }

    fn memory_policy(&self) -> AggregateStateMemoryPolicy {
        AggregateStateMemoryPolicy::AllocationTracked
    }

    fn create_state(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        control.checkpoint(0)?;
        Err(invalid(
            "approximate histogram requires its host state allocator",
        ))
    }

    fn create_state_with_allocator(
        &self,
        allocator: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        observed(control, |work| {
            work.flush()?;
            let allocator = HostAggregateAllocator::try_new(allocator.ok_or_else(|| {
                invalid("approximate histogram requires its host state allocator")
            })?)?;
            let value = match self.profile {
                Profile::Percentile(_) => State::Percentile(PercentileState {
                    digest: Digest::new(allocator.clone()),
                    targets: None,
                }),
                Profile::Frequency => State::Frequency(ApproxTopKState::new(allocator.clone())),
            };
            Ok(ApproxHistogramState { allocator, value })
        })
    }

    fn retained_bytes(&self, _: &Self::State) -> usize {
        // All state allocations are already charged by the actual host.
        0
    }

    fn prepare_update<'a>(
        &'a self,
        input: SelectedAggregateUpdateInput<'a, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedUpdateBatch<'a>, KernelFailure> {
        observed(control, |work| {
            work.step()?;
            if !std::ptr::eq(input.contract(), self.contract.as_ref()) {
                return Err(invalid(
                    "approximate histogram update differs from its exact selected call",
                ));
            }
            Ok(input)
        })
    }

    fn update_row<'a>(
        &self,
        state: &mut Self::State,
        input: &Self::PreparedUpdateBatch<'a>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        let result = observed(control, |work| {
            let row = input
                .selection()
                .row(ordinal)
                .ok_or_else(|| invalid("approximate histogram update ordinal is not selected"))?;
            let mut cells = [(None, 0); 3];
            for (arg, cell) in input.logical_arguments().iter().zip(&mut cells) {
                let array = arg.array().as_ref();
                let index = arg.value_row(ordinal, row);
                work.step()?;
                if index >= array.len() {
                    return Err(invalid(
                        "approximate histogram selected argument address is out of bounds",
                    ));
                }
                if array.is_null(index) {
                    return Ok(());
                }
                *cell = (Some(array), index);
            }
            let cell = |index: usize| {
                let (array, row) = cells[index];
                array.map(|array| (array, row)).ok_or_else(|| {
                    invalid("approximate histogram is missing its selected argument")
                })
            };
            let bigint = |index| {
                let (array, row) = cell(index)?;
                Ok::<_, KernelFailure>(at::<Int64Array>(array)?.value(row))
            };
            match (&self.profile, &mut state.value) {
                (Profile::Percentile(profile), State::Percentile(value)) => {
                    let (samples, row) = cell(0)?;
                    let sample = profile.numeric.sample(samples, row)?;
                    let weight = if profile.weighted {
                        let (weights, row) = cell(1)?;
                        let weight = at::<Float64Array>(weights)?.value(row);
                        if !weight.is_finite() || weight < 1.0 {
                            return Err(invalid_function_argument(
                                "approx_percentile weight must be finite and at least 1",
                            ));
                        }
                        weight
                    } else {
                        1.0
                    };
                    let (targets, row) = cell(if profile.weighted { 2 } else { 1 })?;
                    if profile.array {
                        if value.targets.is_none() {
                            value.targets = Some(Targets::Array(prepare_targets(
                                at::<ListArray>(targets)?,
                                row,
                                &state.allocator,
                                work,
                            )?));
                        }
                    } else {
                        value.targets =
                            Some(Targets::Scalar(at::<Float64Array>(targets)?.value(row)));
                    }
                    value.digest.insert(sample, weight, work)
                }
                (Profile::Frequency, State::Frequency(value)) => {
                    if !value.initialized {
                        value.k = positive_i32(
                            bigint(0)?,
                            2,
                            "approx_most_frequent bucket count must be between 2 and INT_MAX",
                        )?;
                        value.counter_num = positive_i32(
                            bigint(2)?,
                            1,
                            "approx_most_frequent capacity must be between 1 and INT_MAX",
                        )?;
                        value.initialized = true;
                    }
                    let argument = input.logical_arguments()[1];
                    let index = argument.value_row(ordinal, row);
                    let key =
                        tracked_scalar_from_array(argument.array(), index, &state.allocator, work)?;
                    update_one_observed(value, key, 1, work)
                }
                (_, State::Failed) => Err(KernelFailure::InstanceFailed),
                _ => Err(invalid(
                    "approximate histogram cannot update a foreign state profile",
                )),
            }
        });
        if result.is_err() {
            state.value = State::Failed;
        }
        result
    }

    fn prepare_merge<'a>(
        &'a self,
        _: SelectedAggregateMergeInput<'a, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedMergeBatch<'a>, KernelFailure> {
        control.checkpoint(0)?;
        Err(invalid("approximate histogram has no merge phase"))
    }

    fn merge_row<'a>(
        &self,
        _: &mut Self::State,
        _: &Self::PreparedMergeBatch<'a>,
        _: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        control.checkpoint(0)?;
        Err(invalid("approximate histogram has no merge phase"))
    }

    fn build_intermediate<'s, I>(
        &self,
        _: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'s Self::State>,
    {
        control.checkpoint(0)?;
        Err(invalid(
            "approximate histogram has no intermediate emission",
        ))
    }

    fn build_final<'s, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'s Self::State>,
    {
        observed(control, |work| {
            let output = self.contract.final_type().data_type();
            match self.profile {
                Profile::Percentile(profile) => match profile.numeric {
                    NumericInput::Bigint => {
                        emit_percentiles::<Int64Type>(states, output, java_round, work)
                    }
                    NumericInput::Real => {
                        emit_percentiles::<Float32Type>(states, output, |value| value as f32, work)
                    }
                    NumericInput::Double => {
                        emit_percentiles::<Float64Type>(states, output, |value| value, work)
                    }
                },
                Profile::Frequency => emit_frequencies(states, output, work),
            }
        })
    }
}

fn at<T: Array + 'static>(array: &dyn Array) -> Result<&T, KernelFailure> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| invalid("approximate aggregate carrier differs from the selected type"))
}

fn positive_i32(value: i64, minimum: i32, message: &'static str) -> Result<usize, KernelFailure> {
    let value = i32::try_from(value).map_err(|_| invalid_function_argument(message))?;
    if value < minimum {
        return Err(invalid_function_argument(message));
    }
    Ok(value as usize)
}

fn checked_percentile(value: f64) -> Result<f64, KernelFailure> {
    if (0.0..=1.0).contains(&value) {
        Ok(value)
    } else {
        Err(invalid_function_argument(
            "approx_percentile percentile must be between 0 and 1",
        ))
    }
}

fn prepare_targets(
    array: &ListArray,
    row: usize,
    allocator: &HostAggregateAllocator,
    work: &mut ScalarWork<'_, '_>,
) -> Result<StateVec<f64, HostAggregateAllocator>, KernelFailure> {
    let values = at::<Float64Array>(array.values().as_ref())?;
    let offsets = array.value_offsets();
    let begin = offsets[row] as usize;
    let end = offsets[row + 1] as usize;
    let mut targets = aggregate_vec_with_capacity(
        allocator,
        end - begin,
        "reserve approx_percentile target array",
        work,
    )?;
    for index in begin..end {
        work.step()?;
        if values.is_null(index) {
            return Err(invalid_function_argument(
                "approx_percentile percentile array cannot contain NULL",
            ));
        }
        targets.push(checked_percentile(values.value(index))?);
    }
    Ok(targets)
}

fn java_round(value: f64) -> i64 {
    let floor = value.floor();
    (floor as i64).saturating_add(i64::from(value - floor >= 0.5))
}

fn percentile_state(value: &ApproxHistogramState) -> Result<&PercentileState, KernelFailure> {
    match &value.value {
        State::Percentile(state) => Ok(state),
        State::Failed => Err(KernelFailure::InstanceFailed),
        _ => Err(invalid(
            "approx_percentile emission has a foreign state profile",
        )),
    }
}

fn emit_percentiles<'s, T: ArrowPrimitiveType>(
    values: impl ExactSizeIterator<Item = &'s ApproxHistogramState>,
    output: &DataType,
    convert: impl Fn(f64) -> T::Native,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ArrayRef, KernelFailure> {
    work.flush()?;
    if let DataType::List(field) = output {
        let mut builder = ListBuilder::new(PrimitiveBuilder::<T>::new()).with_field(field.clone());
        let mut offset = 0;
        for state in values {
            work.step()?;
            let state = percentile_state(state)?;
            match &state.targets {
                Some(Targets::Array(targets)) => {
                    offset = checked_map_output_offset(offset, targets.len())?;
                    let digest = state.digest.prepare(work)?;
                    for target in targets {
                        builder
                            .values()
                            .append_value(convert(digest.quantile(*target, work)?));
                    }
                    builder.append(true);
                }
                None => builder.append_null(),
                _ => return Err(invalid("approx_percentile expected an array state")),
            }
        }
        work.flush()?;
        return Ok(Arc::new(builder.finish()));
    }
    let mut builder = PrimitiveBuilder::<T>::new();
    for state in values {
        work.step()?;
        let state = percentile_state(state)?;
        match state.targets {
            Some(Targets::Scalar(target)) => {
                let target = checked_percentile(target)?;
                let digest = state.digest.prepare(work)?;
                builder.append_value(convert(digest.quantile(target, work)?));
            }
            None => builder.append_null(),
            _ => return Err(invalid("approx_percentile expected a scalar state")),
        }
    }
    work.flush()?;
    Ok(Arc::new(builder.finish()))
}

enum Keys {
    Bigint(Int64Builder),
    Varchar(StringBuilder),
}

impl Keys {
    fn append(
        &mut self,
        value: &TrackedAggScalarValue<HostAggregateAllocator>,
    ) -> Result<(), KernelFailure> {
        match (self, value) {
            (Self::Bigint(keys), TrackedAggScalarValue::Int64(value)) => keys.append_value(*value),
            (Self::Varchar(keys), TrackedAggScalarValue::Utf8(value)) => keys.append_value(
                std::str::from_utf8(value)
                    .map_err(|_| invalid("approx_most_frequent retained value must be UTF-8"))?,
            ),
            _ => {
                return Err(invalid(
                    "approx_most_frequent output key differs from the selected type",
                ));
            }
        }
        Ok(())
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Bigint(mut keys) => Arc::new(keys.finish()),
            Self::Varchar(mut keys) => Arc::new(keys.finish()),
        }
    }
}

fn emit_frequencies<'s>(
    states: impl ExactSizeIterator<Item = &'s ApproxHistogramState>,
    output: &DataType,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ArrayRef, KernelFailure> {
    let (field, entries, ordered) = parse_map_type(output)?;
    let mut keys = match entries[0].data_type() {
        DataType::Int64 => Keys::Bigint(Int64Builder::new()),
        DataType::Utf8 => Keys::Varchar(StringBuilder::new()),
        _ => return Err(invalid("approx_most_frequent has an uninstalled key type")),
    };
    work.flush()?;
    let mut counts = Int64Builder::new();
    let mut validity = NullBufferBuilder::new(states.len());
    let mut offsets = Vec::with_capacity(states.len() + 1);
    offsets.push(0);
    for state in states {
        let state = match &state.value {
            State::Frequency(state) => state,
            State::Failed => return Err(KernelFailure::InstanceFailed),
            _ => {
                return Err(invalid(
                    "approx_most_frequent emission has a foreign state profile",
                ));
            }
        };
        work.step()?;
        let mut sorted = aggregate_vec_with_capacity(
            &state.allocator,
            state.counts.len(),
            "reserve approx_most_frequent emission ranks",
            work,
        )?;
        for entry in &state.counts {
            work.step()?;
            sorted.push(entry);
        }
        let mut failure = None;
        sorted.sort_unstable_by(|(left_key, left), (right_key, right)| {
            if failure.is_some() {
                return std::cmp::Ordering::Equal;
            }
            if let Err(cause) = work.step() {
                failure = Some(cause);
                return std::cmp::Ordering::Equal;
            }
            right
                .count
                .cmp(&left.count)
                .then_with(|| left_key.cmp(right_key))
        });
        if let Some(cause) = failure {
            return Err(cause);
        }
        work.flush()?;
        let emitted = state.k.min(sorted.len());
        validity.append(emitted > 0);
        let previous = *offsets.last().expect("initial offset");
        offsets.push(checked_map_output_offset(previous, emitted)?);
        for (_, entry) in sorted.into_iter().take(emitted) {
            work.step()?;
            let value = entry
                .value
                .as_ref()
                .ok_or_else(|| invalid("approx_most_frequent retained a NULL key"))?;
            keys.append(value)?;
            counts.append_value(entry.count);
        }
    }
    work.flush()?;
    let values = StructArray::try_new(
        entries,
        vec![keys.finish(), Arc::new(counts.finish())],
        None,
    )
    .map_err(|_| invalid("approx_most_frequent cannot assemble its selected map entries"))?;
    let output = MapArray::try_new(
        field,
        OffsetBuffer::new(offsets.into()),
        values,
        validity.finish(),
        ordered,
    )
    .map_err(|_| invalid("approx_most_frequent cannot assemble its selected map"))?;
    Ok(Arc::new(output))
}
