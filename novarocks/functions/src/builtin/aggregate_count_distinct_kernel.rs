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

//! Selected COUNT DISTINCT through the original family computation and codec.
use super::aggregate_count_distinct_core::{
    self as core, CountBuffer, CountDistinctState, SelectedCountKey, TrackedCountReader,
};
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_scalar::ScalarWork;
use crate::kernel_control::{KernelControlObservation, compile_failure, internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::*;
use allocator_api2::vec::Vec as HostVec;
use arrow_array::{Array, ArrayRef, BinaryArray, builder::Int64Builder};
use arrow_schema::DataType;
use std::sync::Arc;

#[derive(Debug)]
pub(super) struct CountDistinctKernel {
    pub(super) contract: Arc<AggregateCallContract>,
}
#[derive(Debug)]
pub(super) struct CountState {
    values: CountDistinctState<HostAggregateAllocator>,
    failed: bool,
}
impl CountState {
    fn latch_failure(&mut self) {
        self.failed = true;
        self.values.clear();
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
impl PreparedAggregateKernel for CountDistinctKernel {
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
        Ok(Arc::new(Self { contract }))
    }
    type State = CountState;
    type PreparedUpdateBatch<'a> = SelectedAggregateUpdateInput<'a, 'a>;
    type PreparedMergeBatch<'a> = SelectedAggregateMergeInput<'a, 'a>;
    fn contract(&self) -> &Arc<AggregateCallContract> {
        &self.contract
    }
    fn memory_policy(&self) -> AggregateStateMemoryPolicy {
        AggregateStateMemoryPolicy::AllocationTracked
    }
    fn retained_bytes(&self, state: &Self::State) -> usize {
        state.values.allocator.metadata_bytes() + state.values.retained_bytes()
    }
    fn create_state(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        control.checkpoint(0)?;
        Err(invalid(
            "allocation-tracked COUNT DISTINCT requires a host allocator",
        ))
    }
    fn create_state_with_allocator(
        &self,
        allocator: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        observed(control, |work| {
            let host = allocator.ok_or_else(|| {
                invalid("allocation-tracked COUNT DISTINCT requires a host allocator")
            })?;
            work.step()?;
            Ok(CountState {
                values: CountDistinctState::new(HostAggregateAllocator::try_new(host)?),
                failed: false,
            })
        })
    }
    fn prepare_update<'a>(
        &'a self,
        input: Self::PreparedUpdateBatch<'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedUpdateBatch<'a>, KernelFailure> {
        observed(control, |work| {
            let valid = std::ptr::eq(input.contract(), self.contract.as_ref())
                && self.contract.phase().consumes_logical_arguments()
                && input.logical_arguments().len() == self.contract.logical_argument_types().len()
                && !input.logical_arguments().is_empty()
                && input.order_arguments().is_empty();
            work.step()?;
            if !valid {
                return Err(invalid(
                    "COUNT DISTINCT update differs from its exact phase or channels",
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
                .ok_or_else(|| invalid("COUNT DISTINCT selected ordinal is out of bounds"))?;
            work.step()?;
            let allocator = state.values.allocator.clone();
            let buffer = SelectedCountKey::new(allocator.clone());
            let key = if input.logical_arguments().len() == 1 {
                let argument = &input.logical_arguments()[0];
                let address = argument.value_row(ordinal, row);
                if address >= argument.array().len() {
                    return Err(internal("COUNT DISTINCT selected address is out of bounds"));
                }
                core::encode_row(
                    argument.array(),
                    address,
                    &TrackedCountReader(&allocator),
                    buffer,
                    &mut ScalarWork::new(Some(work)),
                )?
            } else {
                let mut arguments = HostVec::new_in(allocator.clone());
                arguments
                    .try_reserve_exact(input.logical_arguments().len())
                    .map_err(|_| arguments.allocator().take_failure())?;
                for argument in input.logical_arguments() {
                    let address = argument.value_row(ordinal, row);
                    work.step()?;
                    if address >= argument.array().len() {
                        return Err(internal("COUNT DISTINCT tuple address is out of bounds"));
                    }
                    arguments.push((argument.array(), address));
                }
                core::encode_tuple(
                    &arguments,
                    &allocator,
                    buffer,
                    &mut ScalarWork::new(Some(work)),
                )?
            };
            if let Some(key) = key {
                state
                    .values
                    .insert_with_work(key.bytes(), &mut ScalarWork::new(Some(work)))?;
            }
            Ok(())
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
            let valid = std::ptr::eq(input.contract(), self.contract.as_ref())
                && !self.contract.phase().consumes_logical_arguments()
                && input.state().array().as_any().is::<BinaryArray>();
            work.step()?;
            if !valid {
                return Err(invalid(
                    "COUNT DISTINCT merge differs from its exact phase or Binary state",
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
                .ok_or_else(|| invalid("COUNT DISTINCT merge ordinal is out of bounds"))?;
            work.step()?;
            let argument = input.state();
            let address = argument.value_row(ordinal, row);
            let array = argument
                .array()
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| internal("COUNT DISTINCT merge carrier is not BinaryArray"))?;
            if address >= array.len() {
                return Err(internal("COUNT DISTINCT merge address is out of bounds"));
            }
            if array.is_null(address) {
                return Ok(());
            }
            let decoded = HostVec::<HostVec<u8, _>, _>::new_in(state.values.allocator.clone());
            let decoded = core::deserialize_set(
                array.value(address),
                decoded,
                &mut ScalarWork::new(Some(work)),
            )?;
            core::merge_decoded(&mut state.values, decoded, &mut ScalarWork::new(Some(work)))
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
            let mut builder = crate::aggregate_scalar::BinaryResultBuilder::new();
            let mut count = 0;
            for state in states {
                if count == expected {
                    return Err(internal(
                        "COUNT DISTINCT state iterator exceeded its extent",
                    ));
                }
                if state.failed {
                    return Err(KernelFailure::InstanceFailed);
                }
                let scratch = HostVec::<u8, _>::new_in(state.values.allocator.clone());
                let buffer =
                    core::serialize_set(&state.values, scratch, &mut ScalarWork::new(Some(work)))?;
                work.flush()?;
                builder.append_value(buffer.bytes())?;
                work.flush()?;
                count += 1;
                work.step()?;
            }
            if count != expected {
                return Err(internal(
                    "COUNT DISTINCT state iterator shortened its extent",
                ));
            }
            work.flush()?;
            let array = Arc::new(builder.finish()?);
            work.flush()?;
            Ok(array as ArrayRef)
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
            let expected = states.len();
            let mut builder = Int64Builder::new();
            let mut count = 0;
            for state in states {
                if count == expected {
                    return Err(internal(
                        "COUNT DISTINCT state iterator exceeded its extent",
                    ));
                }
                if state.failed {
                    return Err(KernelFailure::InstanceFailed);
                }
                work.step()?;
                builder.append_value(core::finalize_count(&state.values)?);
                count += 1;
            }
            if count != expected {
                return Err(internal(
                    "COUNT DISTINCT state iterator shortened its extent",
                ));
            }
            work.flush()?;
            let array = Arc::new(builder.finish());
            work.flush()?;
            Ok(array as ArrayRef)
        })
    }
}
fn nested_supported(
    semantic: &novarocks_type_contract::ValueTypeRef,
    ty: &DataType,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<bool, KernelFailure> {
    work.step().map_err(compile_failure)?;
    if semantic
        .domain()
        .is_some_and(|domain| domain != novarocks_type_contract::ValueLogicalType::LargeInt)
    {
        return Ok(false);
    }
    Ok(match ty {
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::Date32
        | DataType::Timestamp(..)
        | DataType::Decimal128(..)
        | DataType::Decimal256(..)
        | DataType::FixedSizeBinary(16) => true,
        DataType::List(item) => match semantic.child_type_at(0) {
            Some(child) => nested_supported(child, item.data_type(), work)?,
            None => false,
        },
        DataType::Struct(fields) => {
            for (index, field) in fields.iter().enumerate() {
                let Some(child) = semantic.child_type_at(index) else {
                    return Ok(false);
                };
                if !nested_supported(child, field.data_type(), work)? {
                    return Ok(false);
                }
            }
            true
        }
        DataType::Map(entries, _) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Ok(false);
            };
            if fields.len() != 2 {
                return Ok(false);
            }
            for (index, field) in fields.iter().enumerate() {
                let Some(child) = semantic.child_type_at(index) else {
                    return Ok(false);
                };
                if !nested_supported(child, field.data_type(), work)? {
                    return Ok(false);
                }
            }
            true
        }
        _ => false,
    })
}
pub(super) fn validate_contract(
    contract: &AggregateCallContract,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    use novarocks_type_contract::ValueLogicalType;
    let selected = contract.call().selected();
    if selected.argument_types.is_empty() {
        return Err(invalid("COUNT DISTINCT requires value arguments"));
    }
    let tuple = selected.argument_types.len() > 1;
    for source in &selected.argument_types {
        let FunctionArgumentType::Value(source) = source else {
            return Err(invalid("COUNT DISTINCT requires canonical value arguments"));
        };
        if source.logical_type() != ValueLogicalType::Physical
            && !(tuple
                && source.logical_type() == ValueLogicalType::LargeInt
                && matches!(source.data_type(), DataType::FixedSizeBinary(16)))
        {
            return Err(invalid(
                "COUNT DISTINCT source has an unsupported logical identity",
            ));
        }
        let supported = if tuple {
            nested_supported(source.value_type(), source.data_type(), work)?
        } else {
            match source.data_type() {
                DataType::Null
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Boolean
                | DataType::Utf8
                | DataType::Binary
                | DataType::Date32
                | DataType::Timestamp(..)
                | DataType::Decimal128(..)
                | DataType::Decimal256(..) => {
                    work.step().map_err(compile_failure)?;
                    true
                }
                ty @ (DataType::List(_) | DataType::Struct(_)) => {
                    nested_supported(source.value_type(), ty, work)?
                }
                _ => false,
            }
        };
        if !supported {
            return Err(invalid(&format!(
                "COUNT DISTINCT has no installed input profile for {:?}",
                source
            )));
        }
    }
    let FunctionResultType::Scalar(output) = &selected.result_type else {
        return Err(invalid("COUNT DISTINCT requires a scalar result"));
    };
    if output.logical_type() != ValueLogicalType::Physical
        || output.nullable()
        || output.data_type() != &DataType::Int64
        || contract.intermediate_type().logical_type() != ValueLogicalType::Physical
        || contract.intermediate_type().data_type() != &DataType::Binary
    {
        return Err(invalid(
            "COUNT DISTINCT result or intermediate domain differs from its owner",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "aggregate_count_distinct_failure_tests.rs"]
mod failure_tests;
