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

#[path = "approx_distinct_hash.rs"]
mod hash;
#[path = "approx_distinct_sketch.rs"]
mod sketch;

use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_scalar::{BinaryResultBuilder, ScalarWork};
use crate::kernel_control::{KernelControlObservation, invalid, invalid_function_argument};
use crate::kernel_input::EvaluationCheckpoints;
use crate::{
    AggregateCallContract, AggregateKernelPhase, AggregateStateAllocator,
    AggregateStateMemoryPolicy, FunctionArgument, FunctionBindingRequest, FunctionValueType,
    KernelEvaluationControl, KernelFailure, PreparedAggregateKernel, SelectedAggregateMergeInput,
    SelectedAggregateUpdateInput,
};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, builder::Int64Builder,
};
use arrow_schema::DataType;
use hash::HashProfile;
use novarocks_constant_contract::ConstantError;
use novarocks_type_contract::{CompilePhase, PureCompileControl};
use sketch::Sketch;
use std::sync::Arc;

const DEFAULT_ACCURACY: f64 = 0.023;
const DEFAULT_PRECISION: u8 = 11;
const MIN_ACCURACY: f64 = 0.004_062_5;
const MAX_ACCURACY: f64 = 0.26;

fn precision_for_accuracy(accuracy: f64) -> Option<u8> {
    if !(MIN_ACCURACY..=MAX_ACCURACY).contains(&accuracy) {
        return None;
    }
    let buckets = (1.0816 / (accuracy * accuracy)).ceil() as u32;
    Some(buckets.next_power_of_two().trailing_zeros() as u8)
}

/// SQL/physical phase eligibility consumes the exact original static input.
/// Invalid or NULL accuracy leaves the row-domain decision to Single.
// Design: ADR-0275 (docs/adr/ADR-0275-approximate-distinct-freezes-precision-for-split.md)
pub fn fixed_distinct_precision(
    logical_arity: usize,
    accuracy: Option<&FunctionArgument>,
) -> Result<Option<u8>, ConstantError> {
    match (logical_arity, accuracy) {
        (1, None) => Ok(Some(DEFAULT_PRECISION)),
        (
            2,
            Some(FunctionArgument::Value {
                value_type,
                constant: Some(value),
            }),
        ) if value_type.data_type() == &DataType::Float64 => Ok(value
            .try_f64_bits()?
            .and_then(|bits| precision_for_accuracy(f64::from_bits(bits)))),
        _ => Ok(None),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ApproxProfile {
    Unknown,
    Boolean,
    Hash(HashProfile),
}

impl ApproxProfile {
    pub(super) fn prepare(value: &FunctionValueType) -> Option<Self> {
        match value.data_type() {
            DataType::Null => Some(Self::Unknown),
            DataType::Boolean => Some(Self::Boolean),
            _ => HashProfile::prepare(value).map(Self::Hash),
        }
    }

    fn new_state(&self) -> State {
        match self {
            Self::Unknown => State::Unknown,
            Self::Boolean => State::Boolean(0),
            Self::Hash(_) => State::Hash(None),
        }
    }
}

enum State {
    Unknown,
    Boolean(u8),
    Hash(Option<Sketch<HostAggregateAllocator>>),
    Failed,
}

pub(crate) struct ApproxDistinctState {
    allocator: HostAggregateAllocator,
    value: State,
}

#[derive(Debug)]
pub(crate) struct ApproxDistinctKernel {
    contract: Arc<AggregateCallContract>,
    profile: ApproxProfile,
    arity: u8,
    fixed_precision: Option<u8>,
}

impl ApproxDistinctKernel {
    pub(super) fn prepare(
        contract: Arc<AggregateCallContract>,
        request: FunctionBindingRequest<'_>,
    ) -> Result<Self, KernelFailure> {
        let logical_arity = request.logical_argument_count;
        let profile = contract
            .logical_argument_types()
            .next()
            .and_then(ApproxProfile::prepare)
            .ok_or_else(|| invalid("approx_distinct requires its installed input profile"))?;
        if !(1..=2).contains(&logical_arity)
            || contract.logical_argument_types().len() != logical_arity
            || request.arguments.len() != logical_arity
        {
            return Err(invalid(
                "approx_distinct requires its exact unordered argument shape",
            ));
        }
        let fixed_precision = fixed_distinct_precision(logical_arity, request.arguments.get(1))
            .map_err(|_| {
                invalid("approx_distinct accuracy differs from its exact constant carrier")
            })?;
        if contract.phase() != AggregateKernelPhase::Single && fixed_precision.is_none() {
            return Err(invalid(
                "approx_distinct stages require fixed legal accuracy",
            ));
        }
        Ok(Self {
            contract,
            profile,
            arity: logical_arity as u8,
            fixed_precision,
        })
    }

    fn phase_precision(&self) -> Result<u8, KernelFailure> {
        self.fixed_precision.ok_or_else(|| {
            invalid("approx_distinct has no fixed precision for merging or emitting state")
        })
    }
}

fn first_precision(arity: u8, fixed: Option<u8>, accuracy: f64) -> Result<u8, KernelFailure> {
    let precision = if arity == 1 {
        DEFAULT_PRECISION
    } else {
        precision_for_accuracy(accuracy).ok_or_else(|| {
            invalid_function_argument("Max standard error must be in [0.0040625, 0.26]")
        })?
    };
    if fixed.is_some_and(|expected| expected != precision) {
        return Err(invalid(
            "approx_distinct runtime accuracy disagrees with its fixed precision",
        ));
    }
    Ok(precision)
}

pub(super) fn observed<T>(
    control: &dyn KernelEvaluationControl,
    f: impl FnOnce(&mut ScalarWork<'_, '_>) -> Result<T, KernelFailure>,
) -> Result<T, KernelFailure> {
    let observation = KernelControlObservation::new(control);
    observation.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(&observation);
    let result = f(&mut ScalarWork::new(Some(&mut work)));
    observation.finish(work.finish_result(result))
}

impl PreparedAggregateKernel for ApproxDistinctKernel {
    type State = ApproxDistinctState;
    type PreparedUpdateBatch<'a> = SelectedAggregateUpdateInput<'a, 'a>;
    type PreparedMergeBatch<'a> = SelectedAggregateMergeInput<'a, 'a>;

    fn contract(&self) -> &Arc<AggregateCallContract> {
        &self.contract
    }

    fn supports_local_stages(&self) -> bool {
        self.fixed_precision.is_some()
            && !self.contract.distinct()
            && self.contract.order_keys().is_empty()
    }
    fn clone_for_local_phase(
        &self,
        contract: Arc<AggregateCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<Self>, KernelFailure> {
        control
            .checkpoint(CompilePhase::FunctionSpecialization, 0)
            .map_err(crate::kernel_control::compile_failure)?;
        self.phase_precision()?;
        Ok(Arc::new(Self {
            contract,
            profile: self.profile.clone(),
            arity: self.arity,
            fixed_precision: self.fixed_precision,
        }))
    }

    fn memory_policy(&self) -> AggregateStateMemoryPolicy {
        AggregateStateMemoryPolicy::AllocationTracked
    }

    fn create_state(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        control.checkpoint(0)?;
        Err(invalid("approx_distinct requires its host state allocator"))
    }

    fn create_state_with_allocator(
        &self,
        allocator: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        observed(control, |work| {
            work.flush()?;
            let allocator =
                HostAggregateAllocator::try_new(allocator.ok_or_else(|| {
                    invalid("approx_distinct requires its host state allocator")
                })?)?;
            let value = self.profile.new_state();
            Ok(ApproxDistinctState { allocator, value })
        })
    }

    fn retained_bytes(&self, state: &Self::State) -> usize {
        state.allocator.metadata_bytes()
            + match &state.value {
                State::Hash(Some(sketch)) => sketch.retained_bytes(),
                _ => 0,
            }
    }

    fn prepare_update<'a>(
        &'a self,
        input: Self::PreparedUpdateBatch<'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedUpdateBatch<'a>, KernelFailure> {
        observed(control, |work| {
            work.step()?;
            if !std::ptr::eq(input.contract(), self.contract.as_ref())
                || !self.contract.phase().consumes_logical_arguments()
                || input.logical_arguments().len() != usize::from(self.arity)
                || !input.order_arguments().is_empty()
            {
                return Err(invalid(
                    "approx_distinct update differs from its exact selected call",
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
            if matches!(state.value, State::Failed) {
                return Err(KernelFailure::InstanceFailed);
            }
            let row = input
                .selection()
                .row(ordinal)
                .ok_or_else(|| invalid("approx_distinct update ordinal is not selected"))?;
            let arguments = input.logical_arguments();
            let value = arguments[0];
            let array = value.array();
            let index = value.value_row(ordinal, row);
            work.step()?;
            if index >= array.len() {
                return Err(invalid(
                    "approx_distinct selected value address is out of bounds",
                ));
            }
            // UNKNOWN has Arrow's NullArray carrier with no validity
            // bitmap. Its logical rows contribute nothing.
            if matches!(state.value, State::Unknown) {
                return if array.data_type() == &DataType::Null {
                    Ok(())
                } else {
                    Err(invalid("approx_distinct UNKNOWN carrier mismatch"))
                };
            }
            if array.is_null(index) {
                return Ok(());
            }
            let accuracy = if let Some(accuracy) = arguments.get(1) {
                let array = accuracy
                    .array()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| invalid("approx_distinct accuracy requires a DOUBLE carrier"))?;
                let index = accuracy.value_row(ordinal, row);
                if index >= array.len() {
                    return Err(invalid(
                        "approx_distinct accuracy address exceeds its selected array",
                    ));
                }
                if array.is_null(index) {
                    return Ok(());
                }
                array.value(index)
            } else {
                DEFAULT_ACCURACY
            };
            match (&mut state.value, &self.profile) {
                (State::Boolean(bits), ApproxProfile::Boolean) => {
                    let array = array
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| invalid("approx_distinct BOOLEAN carrier mismatch"))?;
                    *bits |= 1 << u8::from(array.value(index));
                }
                (State::Hash(current), ApproxProfile::Hash(profile)) => {
                    if current.is_none() {
                        let precision =
                            first_precision(self.arity, self.fixed_precision, accuracy)?;
                        *current = Some(Sketch::new(precision, state.allocator.clone()));
                    }
                    let sketch = current
                        .as_mut()
                        .expect("first contribution establishes precision");
                    let hash = profile.hash(array.as_ref(), index, work)?.ok_or_else(|| {
                        invalid("approx_distinct contribution contains an unexpected NULL")
                    })?;
                    sketch.add_hash(hash, work)?;
                }
                _ => {
                    return Err(invalid(
                        "approx_distinct cannot update a foreign state profile",
                    ));
                }
            }
            Ok(())
        });
        if result.is_err() {
            state.value = State::Failed;
        }
        result
    }

    fn prepare_merge<'a>(
        &'a self,
        input: Self::PreparedMergeBatch<'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedMergeBatch<'a>, KernelFailure> {
        observed(control, |work| {
            work.step()?;
            if !std::ptr::eq(input.contract(), self.contract.as_ref())
                || self.contract.phase().consumes_logical_arguments()
            {
                return Err(invalid(
                    "approx_distinct merge differs from its exact selected state",
                ));
            }
            Ok(input)
        })
    }

    fn merge_row<'a>(
        &self,
        state: &mut Self::State,
        input: &Self::PreparedMergeBatch<'a>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        let result = observed(control, |work| {
            let row = input
                .selection()
                .row(ordinal)
                .ok_or_else(|| invalid("approx_distinct merge ordinal is not selected"))?;
            let argument = input.state();
            let array = argument
                .array()
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    invalid("approx_distinct merge requires its binary state carrier")
                })?;
            let index = argument.value_row(ordinal, row);
            work.step()?;
            if index >= array.len() || array.is_null(index) {
                return Err(invalid(
                    "approx_distinct has no state at the selected merge address",
                ));
            }
            let payload = array.value(index);
            let precision = self.phase_precision()?;
            match (&mut state.value, &self.profile) {
                (State::Unknown, ApproxProfile::Unknown) => {
                    if payload == [b'N', b'U', 2, self.arity, precision] {
                        Ok(())
                    } else {
                        Err(invalid("approx_distinct UNKNOWN state format mismatch"))
                    }
                }
                (State::Boolean(bits), ApproxProfile::Boolean) => {
                    let [b'N', b'B', 2, arity, actual_precision, value] = payload else {
                        return Err(invalid("approx_distinct BOOLEAN state format mismatch"));
                    };
                    if *arity != self.arity || *actual_precision != precision || *value > 3 {
                        return Err(invalid(
                            "approx_distinct BOOLEAN state is invalid for this call",
                        ));
                    }
                    *bits |= *value;
                    Ok(())
                }
                (State::Hash(current), ApproxProfile::Hash(profile)) => current
                    .get_or_insert_with(|| Sketch::new(precision, state.allocator.clone()))
                    .merge(payload, self.arity, profile.transport_profile(), work),
                (State::Failed, _) => Err(KernelFailure::InstanceFailed),
                _ => Err(invalid("approx_distinct has a foreign merge profile")),
            }
        });
        if result.is_err() {
            state.value = State::Failed;
        }
        result
    }

    fn build_intermediate<'s, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'s Self::State>,
    {
        observed(control, |work| {
            if self.contract.phase().produces_final_result() {
                return Err(invalid(
                    "approx_distinct final phase cannot emit intermediate state",
                ));
            }
            let precision = self.phase_precision()?;
            let mut output = BinaryResultBuilder::new();
            for state in states {
                work.step()?;
                match (&state.value, &self.profile) {
                    (State::Unknown, ApproxProfile::Unknown) => {
                        output.append_value([b'N', b'U', 2, self.arity, precision])?;
                    }
                    (State::Boolean(bits), ApproxProfile::Boolean) => {
                        output.append_value([b'N', b'B', 2, self.arity, precision, *bits])?;
                    }
                    (State::Hash(current), ApproxProfile::Hash(profile)) => {
                        let bytes = if let Some(sketch) = current {
                            sketch.encode(self.arity, profile.transport_profile(), work)?
                        } else {
                            Sketch::new(precision, state.allocator.clone()).encode(
                                self.arity,
                                profile.transport_profile(),
                                work,
                            )?
                        };
                        work.flush()?;
                        output.append_value(&bytes)?;
                    }
                    (State::Failed, _) => return Err(KernelFailure::InstanceFailed),
                    _ => {
                        return Err(invalid(
                            "approx_distinct has a foreign output state profile",
                        ));
                    }
                }
            }
            Ok(Arc::new(output.finish()?) as ArrayRef)
        })
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
            if !self.contract.phase().produces_final_result() {
                return Err(invalid(
                    "approx_distinct intermediate phase cannot emit a final result",
                ));
            }
            let mut output = Int64Builder::with_capacity(states.len());
            for state in states {
                work.step()?;
                let result = match (&state.value, &self.profile) {
                    (State::Unknown, ApproxProfile::Unknown) => 0,
                    (State::Boolean(bits), ApproxProfile::Boolean) => i64::from(bits.count_ones()),
                    (State::Hash(Some(sketch)), ApproxProfile::Hash(_)) => sketch.estimate(work)?,
                    (State::Hash(None), ApproxProfile::Hash(_)) => 0,
                    (State::Failed, _) => return Err(KernelFailure::InstanceFailed),
                    _ => {
                        return Err(invalid(
                            "approx_distinct has a foreign output state profile",
                        ));
                    }
                };
                output.append_value(result);
            }
            Ok(Arc::new(output.finish()) as ArrayRef)
        })
    }
}

#[cfg(test)]
#[path = "approx_distinct_kernel_tests.rs"]
mod tests;
