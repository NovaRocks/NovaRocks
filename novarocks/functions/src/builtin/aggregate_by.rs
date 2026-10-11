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

//! Exact MAX_BY/MIN_BY over the original tracked scalar and recursive codec.
use super::aggregate_by_core::{ByDirection, ByEncodeBuffer, ByState};
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_scalar::{self as scalar, ScalarStateError, ScalarWork};
use crate::kernel_control::{KernelControlObservation, compile_failure, internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::*;
use allocator_api2::vec::Vec as HostVec;
use arrow_array::{Array, ArrayRef, BinaryArray, builder::BinaryBuilder};
use arrow_schema::DataType;
use std::sync::Arc;
#[derive(Debug)]
pub(super) struct ByKernel {
    pub(super) contract: Arc<AggregateCallContract>,
    pub(super) direction: ByDirection,
}
pub(super) struct HostByBuffer(pub(super) HostVec<u8, HostAggregateAllocator>);
impl ByEncodeBuffer for HostByBuffer {
    fn push(&mut self, byte: u8) -> Result<(), ScalarStateError> {
        self.0
            .try_reserve(1)
            .map_err(|_| ScalarStateError::Kernel(self.0.allocator().take_failure()))?;
        self.0.push(byte);
        Ok(())
    }
    fn append(
        &mut self,
        bytes: &[u8],
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), ScalarStateError> {
        work.flush()?;
        self.0
            .try_reserve(bytes.len())
            .map_err(|_| ScalarStateError::Kernel(self.0.allocator().take_failure()))?;
        work.flush()?;
        for byte in bytes {
            work.step()?;
            self.0.push(*byte);
        }
        Ok(())
    }
}
fn observed<T>(
    control: &dyn KernelEvaluationControl,
    f: impl FnOnce(&mut EvaluationCheckpoints<'_>) -> Result<T, KernelFailure>,
) -> Result<T, KernelFailure> {
    let observation = KernelControlObservation::new(control);
    observation.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(&observation);
    let result = f(&mut work);
    let result = work.finish_result(result);
    observation.finish(result)
}
impl ByKernel {
    /// The one original emitter before either caller projects its error.
    /// LegacyData remains full text here; the old aggregate entry still uses
    /// exactly its original KernelFailure boundary below.
    pub(super) fn build_final_scalar<'s, I>(
        &self,
        states: I,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<ArrayRef, ScalarStateError>
    where
        I: ExactSizeIterator<Item = &'s ByState<HostAggregateAllocator>>,
    {
        let expected = states.len();
        let mut values = Vec::new();
        values
            .try_reserve_exact(expected)
            .map_err(|_| ScalarStateError::Kernel(KernelFailure::ResourceExhausted))?;
        for state in states {
            if values.len() == expected {
                return Err(internal("max_by/min_by state iterator exceeded its extent").into());
            }
            if state.failed {
                return Err(KernelFailure::InstanceFailed.into());
            }
            work.step()?;
            values.push(state.output(work)?);
        }
        if values.len() != expected {
            return Err(internal("max_by/min_by state iterator shortened its extent").into());
        }
        scalar::build_scalar_array(self.output_type(), values, work)
    }
    /// ONE original row update before the caller's error projection/latch.
    pub(super) fn update_row_scalar<'a>(
        &self,
        state: &mut ByState<HostAggregateAllocator>,
        input: &SelectedAggregateUpdateInput<'a, 'a>,
        ordinal: usize,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), ScalarStateError> {
        let row = input
            .selection()
            .row(ordinal)
            .ok_or_else(|| invalid("max_by/min_by selected ordinal is out of bounds"))?;
        work.step()?;
        let values = input.logical_arguments()[0];
        let keys = input.logical_arguments()[1];
        let value_row = values.value_row(ordinal, row);
        let key_row = keys.value_row(ordinal, row);
        if value_row >= values.array().len() || key_row >= keys.array().len() {
            return Err(internal("max_by/min_by selected address is out of bounds").into());
        }
        state.update_from_arrays(
            self.direction,
            values.array(),
            value_row,
            keys.array(),
            key_row,
            work,
        )
    }
    fn output_type(&self) -> &DataType {
        let FunctionResultType::Scalar(output) = &self.contract.call().selected().result_type
        else {
            unreachable!("validated scalar")
        };
        &output.data_type
    }
}
impl PreparedAggregateKernel for ByKernel {
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
            direction: self.direction,
        }))
    }
    type State = ByState<HostAggregateAllocator>;
    type PreparedUpdateBatch<'a> = SelectedAggregateUpdateInput<'a, 'a>;
    type PreparedMergeBatch<'a> = SelectedAggregateMergeInput<'a, 'a>;
    fn contract(&self) -> &Arc<AggregateCallContract> {
        &self.contract
    }
    fn memory_policy(&self) -> AggregateStateMemoryPolicy {
        AggregateStateMemoryPolicy::AllocationTracked
    }
    fn retained_bytes(&self, state: &Self::State) -> usize {
        state.allocator.metadata_bytes() + state.retained_bytes()
    }
    fn create_state(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        control.checkpoint(0)?;
        Err(invalid(
            "allocation-tracked max_by/min_by requires a host allocator",
        ))
    }
    fn create_state_with_allocator(
        &self,
        allocator: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        observed(control, |work| {
            let allocator = allocator.ok_or_else(|| {
                invalid("allocation-tracked max_by/min_by requires a host allocator")
            })?;
            work.step()?;
            Ok(ByState::new(HostAggregateAllocator::try_new(allocator)?))
        })
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
                || input.logical_arguments().len() != 2
                || !input.order_arguments().is_empty()
            {
                return Err(invalid(
                    "max_by/min_by update differs from its exact phase or channels",
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
        if state.failed {
            return Err(KernelFailure::InstanceFailed);
        }
        let result = observed(control, |work| {
            self.update_row_scalar(state, input, ordinal, &mut ScalarWork::new(Some(work)))
                .map_err(ScalarStateError::into_kernel_failure)
        });
        if result.is_err() {
            state.latch_failure();
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
                || input.state().array().data_type() != &DataType::Binary
            {
                return Err(invalid(
                    "max_by/min_by merge differs from its exact phase or Binary state",
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
        if state.failed {
            return Err(KernelFailure::InstanceFailed);
        }
        let result = observed(control, |work| {
            let row = input
                .selection()
                .row(ordinal)
                .ok_or_else(|| invalid("max_by/min_by merge ordinal is out of bounds"))?;
            work.step()?;
            let argument = input.state();
            let address = argument.value_row(ordinal, row);
            let array = argument
                .array()
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| internal("max_by/min_by expected exact BinaryArray state"))?;
            if address >= array.len() {
                return Err(internal("max_by/min_by merge address is out of bounds"));
            }
            if array.is_null(address) {
                return Ok(());
            }
            state
                .merge_bytes(
                    self.direction,
                    array.value(address),
                    &mut ScalarWork::new(Some(work)),
                )
                .map_err(ScalarStateError::into_kernel_failure)
        });
        if result.is_err() {
            state.latch_failure();
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
            let expected = states.len();
            let mut count = 0;
            let mut builder = BinaryBuilder::new();
            let mut work = ScalarWork::new(Some(work));
            for state in states {
                if count == expected {
                    return Err(internal("max_by/min_by state iterator exceeded its extent"));
                }
                count += 1;
                if state.failed {
                    return Err(KernelFailure::InstanceFailed);
                }
                work.step().map_err(ScalarStateError::into_kernel_failure)?;
                let mut bytes = HostByBuffer(HostVec::new_in(state.allocator.clone()));
                if state
                    .serialize(&mut bytes, &mut work)
                    .map_err(ScalarStateError::into_kernel_failure)?
                {
                    work.flush()
                        .map_err(ScalarStateError::into_kernel_failure)?;
                    builder.append_value(&bytes.0);
                    work.flush()
                        .map_err(ScalarStateError::into_kernel_failure)?;
                } else {
                    builder.append_null();
                }
            }
            if count != expected {
                return Err(internal(
                    "max_by/min_by state iterator shortened its extent",
                ));
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
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
            self.build_final_scalar(states, &mut ScalarWork::new(Some(work)))
                .map_err(ScalarStateError::into_kernel_failure)
        })
    }
}
/// The installed aggregate CPU and the original generic window have different
/// failure boundaries. Only the private window route admits original reader
/// Data so that its whole-invocation author can retain the complete message.
#[derive(Clone, Copy)]
pub(super) enum ByPreparationDomain {
    InstalledAggregate,
    OriginalWindow,
}
pub(super) fn validate_contract(
    contract: &AggregateCallContract,
    domain: ByPreparationDomain,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    let [
        FunctionArgumentType::Value(value),
        FunctionArgumentType::Value(key),
    ] = contract.call().selected().argument_types.as_ref()
    else {
        return Err(invalid(
            "max_by/min_by requires exactly two value arguments",
        ));
    };
    let FunctionResultType::Scalar(output) = &contract.call().selected().result_type else {
        return Err(invalid("max_by/min_by requires a scalar result"));
    };
    // Preserve the installed aggregate domain. The private original window
    // keeps generic reader failures at their original whole-call Data boundary;
    // neither route changes the catalogue derivation or exact result type.
    let installed_reader_required = match domain {
        ByPreparationDomain::InstalledAggregate => true,
        ByPreparationDomain::OriginalWindow => false,
    };
    if installed_reader_required
        && (!super::aggregate_any_value::supported(&value.data_type, work)?
            || !super::aggregate_any_value::supported(&key.data_type, work)?)
    {
        return Err(invalid(&format!(
            "{} has no installed max_by/min_by input profile for {:?}",
            contract.call().function_id().as_str(),
            contract.call().selected().argument_types
        )));
    }
    let mut expected = value.clone();
    expected.nullable = true;
    if !expected
        .exactly_equals_observed::<KernelFailure>(output, || work.step().map_err(compile_failure))?
        || !FunctionValueType::new(DataType::Binary, true)
            .exactly_equals_observed::<KernelFailure>(contract.intermediate_type(), || {
                work.step().map_err(compile_failure)
            })?
    {
        return Err(invalid(
            "max_by/min_by selected result or intermediate differs from its exact value and Binary state",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "aggregate_by_failure_tests.rs"]
mod failure_tests;
