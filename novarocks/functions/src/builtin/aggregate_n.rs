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

//! Exact MAX_N/MIN_N over the original tracked scalar and scalar codec.
use super::aggregate_by::HostByBuffer;
use super::aggregate_n_core::NState;
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
pub(super) struct NKernel {
    pub(super) contract: Arc<AggregateCallContract>,
    pub(super) keep_smallest: bool,
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
impl NKernel {
    fn output_type(&self) -> &DataType {
        let FunctionResultType::Scalar(output) = &self.contract.call().selected().result_type
        else {
            unreachable!("validated scalar")
        };
        &output.data_type
    }
}
impl PreparedAggregateKernel for NKernel {
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
            keep_smallest: self.keep_smallest,
        }))
    }
    type State = NState<HostAggregateAllocator>;
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
            "allocation-tracked min_n/max_n requires a host allocator",
        ))
    }
    fn create_state_with_allocator(
        &self,
        allocator: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        observed(control, |work| {
            let allocator = allocator.ok_or_else(|| {
                invalid("allocation-tracked min_n/max_n requires a host allocator")
            })?;
            work.step()?;
            Ok(NState::new(HostAggregateAllocator::try_new(allocator)?))
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
                    "min_n/max_n update differs from its exact phase or channels",
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
            let row = input
                .selection()
                .row(ordinal)
                .ok_or_else(|| invalid("min_n/max_n selected ordinal is out of bounds"))?;
            work.step()?;
            let values = input.logical_arguments()[0];
            let keys = input.logical_arguments()[1];
            let value_row = values.value_row(ordinal, row);
            let key_row = keys.value_row(ordinal, row);
            if value_row >= values.array().len() || key_row >= keys.array().len() {
                return Err(internal("min_n/max_n selected address is out of bounds"));
            }
            state
                .update_from_arrays(
                    values.array(),
                    value_row,
                    keys.array(),
                    key_row,
                    self.keep_smallest,
                    &mut ScalarWork::new(Some(work)),
                )
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
                    "min_n/max_n merge differs from its exact phase or Binary state",
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
                .ok_or_else(|| invalid("min_n/max_n merge ordinal is out of bounds"))?;
            work.step()?;
            let argument = input.state();
            let address = argument.value_row(ordinal, row);
            let array = argument
                .array()
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| internal("min_n/max_n expected exact BinaryArray state"))?;
            if address >= array.len() {
                return Err(internal("min_n/max_n merge address is out of bounds"));
            }
            if array.is_null(address) {
                return Ok(());
            }
            state
                .merge_from_array(
                    argument.array(),
                    address,
                    self.keep_smallest,
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
        self.build_intermediate_checked(states.map(Ok), control)
    }
    fn build_final<'s, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'s Self::State>,
    {
        self.build_final_checked(states.map(Ok), control)
    }
}
impl NKernel {
    pub(super) fn build_intermediate_checked<'s, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = Result<&'s NState<HostAggregateAllocator>, KernelFailure>>,
    {
        observed(control, |work| {
            let expected = states.len();
            let mut count = 0;
            let mut builder = BinaryBuilder::new();
            let mut work = ScalarWork::new(Some(work));
            for state in states {
                let state = state?;
                if count == expected {
                    return Err(internal("min_n/max_n state iterator exceeded its extent"));
                }
                count += 1;
                if state.failed {
                    return Err(KernelFailure::InstanceFailed);
                }
                work.step().map_err(ScalarStateError::into_kernel_failure)?;
                let mut bytes = HostByBuffer(HostVec::new_in(state.allocator.clone()));
                state
                    .serialize(&mut bytes, &mut work)
                    .map_err(ScalarStateError::into_kernel_failure)?;
                work.flush()
                    .map_err(ScalarStateError::into_kernel_failure)?;
                builder.append_value(&bytes.0);
                work.flush()
                    .map_err(ScalarStateError::into_kernel_failure)?;
            }
            if count != expected {
                return Err(internal("min_n/max_n state iterator shortened its extent"));
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        })
    }
    pub(super) fn build_final_checked<'s, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = Result<&'s NState<HostAggregateAllocator>, KernelFailure>>,
    {
        observed(control, |work| {
            let expected = states.len();
            let mut values = Vec::new();
            values
                .try_reserve_exact(expected)
                .map_err(|_| KernelFailure::ResourceExhausted)?;
            let mut work = ScalarWork::new(Some(work));
            for state in states {
                let state = state?;
                if values.len() == expected {
                    return Err(internal("min_n/max_n state iterator exceeded its extent"));
                }
                if state.failed {
                    return Err(KernelFailure::InstanceFailed);
                }
                work.step().map_err(ScalarStateError::into_kernel_failure)?;
                values.push(
                    state
                        .output(&mut work)
                        .map(Some)
                        .map_err(ScalarStateError::into_kernel_failure)?,
                );
            }
            if values.len() != expected {
                return Err(internal("min_n/max_n state iterator shortened its extent"));
            }
            scalar::build_scalar_array(self.output_type(), values, &mut work)
                .map_err(ScalarStateError::into_kernel_failure)
        })
    }
}

pub(super) fn validate_contract(
    contract: &AggregateCallContract,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    let [
        FunctionArgumentType::Value(value),
        FunctionArgumentType::Value(limit),
    ] = contract.call().selected().argument_types.as_ref()
    else {
        return Err(invalid("min_n/max_n requires exactly two value arguments"));
    };
    let FunctionResultType::Scalar(output) = &contract.call().selected().result_type else {
        return Err(invalid("min_n/max_n requires a scalar result"));
    };
    work.step().map_err(compile_failure)?;
    if value.logical_type != novarocks_type_contract::ValueLogicalType::Physical {
        return Err(invalid(&format!(
            "{} has no installed min_n/max_n logical input profile for {:?}",
            contract.call().function_id().as_str(),
            value.logical_type
        )));
    }
    // The installed profile must close the original scalar state codec in
    // every phase; legacy Single-only nested/Binary slices remain outside it.
    let source = matches!(
        value.data_type,
        DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::Date32
            | DataType::Timestamp(..)
            | DataType::Decimal128(..)
            | DataType::Decimal256(..)
            | DataType::FixedSizeBinary(16)
    );
    let integer = limit.logical_type == novarocks_type_contract::ValueLogicalType::Physical
        && matches!(
            limit.data_type,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        );
    if !source || !integer {
        return Err(invalid(&format!(
            "{} has no installed min_n/max_n input profile for {:?}",
            contract.call().function_id().as_str(),
            contract.call().selected().argument_types
        )));
    }
    let expected = FunctionValueType::new(
        DataType::List(Arc::new(super::signature::value_field("item", value, true))),
        true,
    );
    if !expected
        .exactly_equals_observed::<KernelFailure>(output, || work.step().map_err(compile_failure))?
        || !FunctionValueType::new(DataType::Binary, true)
            .exactly_equals_observed::<KernelFailure>(contract.intermediate_type(), || {
                work.step().map_err(compile_failure)
            })?
    {
        return Err(invalid(
            "min_n/max_n selected result or intermediate differs from its exact nullable List and Binary state",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "aggregate_n_failure_tests.rs"]
mod failure_tests;
