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

//! Selected four-phase numeric DISTINCT through the shared v1 computation.
use super::aggregate_distinct_numeric::{self as core, NumericDistinctSet};
use super::aggregate_distinct_storage::NumericDistinctState;
use crate::aggregate_scalar::ScalarStateAllocator;
use crate::kernel_control::{KernelControlObservation, internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::*;
use allocator_api2::vec::Vec as HostVec;
use arrow_array::{Array, ArrayRef, BinaryArray, builder::BinaryBuilder, new_empty_array};
use arrow_schema::DataType;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum DistinctOperation {
    Sum,
    Avg,
}
impl DistinctOperation {
    pub(super) fn arity(self) -> usize {
        1
    }
}
pub(super) fn operation(name: &str) -> Option<DistinctOperation> {
    match name {
        "multi_distinct_sum" => Some(DistinctOperation::Sum),
        "multi_distinct_avg" => Some(DistinctOperation::Avg),
        _ => None,
    }
}
#[derive(Debug)]
pub(super) struct DistinctNumericKernel {
    pub(super) contract: Arc<AggregateCallContract>,
    pub(super) operation: DistinctOperation,
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
impl DistinctNumericKernel {
    fn source_type(&self) -> &DataType {
        let FunctionArgumentType::Value(source) =
            &self.contract.call().selected().argument_types[0]
        else {
            unreachable!("validated numeric value argument")
        };
        source.data_type()
    }
    fn output_type(&self) -> &DataType {
        let FunctionResultType::Scalar(result) = &self.contract.call().selected().result_type
        else {
            unreachable!("validated scalar result")
        };
        result.data_type()
    }
}
impl PreparedAggregateKernel for DistinctNumericKernel {
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
    type State = NumericDistinctState;
    type PreparedUpdateBatch<'a> = SelectedAggregateUpdateInput<'a, 'a>;
    type PreparedMergeBatch<'a> = SelectedAggregateMergeInput<'a, 'a>;
    fn contract(&self) -> &Arc<AggregateCallContract> {
        &self.contract
    }
    fn memory_policy(&self) -> AggregateStateMemoryPolicy {
        AggregateStateMemoryPolicy::AllocationTracked
    }
    fn retained_bytes(&self, state: &Self::State) -> usize {
        state.retained_bytes()
    }
    fn create_state(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        control.checkpoint(0)?;
        Err(invalid(
            "allocation-tracked numeric DISTINCT requires a host allocator",
        ))
    }
    fn create_state_with_allocator(
        &self,
        allocator: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        observed(control, |work| {
            let host = allocator.ok_or_else(|| {
                invalid("allocation-tracked numeric DISTINCT requires a host allocator")
            })?;
            work.step()?;
            NumericDistinctState::new(host)
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
                && input.logical_arguments().len() == 1
                && input.order_arguments().is_empty();
            work.step()?;
            if !valid {
                return Err(invalid(
                    "numeric DISTINCT update differs from its exact phase or channels",
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
                .ok_or_else(|| invalid("numeric DISTINCT selected ordinal is out of bounds"))?;
            work.step()?;
            let argument = &input.logical_arguments()[0];
            let address = argument.value_row(ordinal, row);
            let array = argument.array().as_ref();
            if address >= array.len() {
                return Err(internal(
                    "numeric DISTINCT selected address is out of bounds",
                ));
            }
            if let Some(key) = core::encode_numeric_row(array, address, work)
                .map_err(core::DistinctComputationError::into_kernel_failure)?
            {
                state
                    .insert(key.as_bytes(), work)
                    .map_err(core::DistinctComputationError::into_kernel_failure)?;
            }
            Ok(())
        });
        if result.is_err() {
            state.failed = true;
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
                    "numeric DISTINCT merge differs from its exact phase or Binary state",
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
                .ok_or_else(|| invalid("numeric DISTINCT merge ordinal is out of bounds"))?;
            work.step()?;
            let argument = input.state();
            let address = argument.value_row(ordinal, row);
            let array = argument
                .array()
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| internal("numeric DISTINCT merge carrier is not BinaryArray"))?;
            if address >= array.len() {
                return Err(internal("numeric DISTINCT merge address is out of bounds"));
            }
            if array.is_null(address) {
                return Ok(());
            }
            let width = core::numeric_key_width(self.source_type())
                .map_err(core::DistinctComputationError::into_kernel_failure)?;
            // Decoder validates the whole payload before the first insertion.
            core::visit_serialized_keys_with_work(
                array.value(address),
                width,
                work,
                |bytes, work| state.insert(bytes, work),
            )
            .map_err(core::DistinctComputationError::into_kernel_failure)
        });
        if result.is_err() {
            state.failed = true;
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
            let mut builder = BinaryBuilder::new();
            let mut payload_bytes = 0usize;
            let mut rows = 0usize;
            for state in states {
                if rows == expected {
                    return Err(internal(
                        "numeric DISTINCT state iterator exceeded its extent",
                    ));
                }
                if state.failed {
                    return Err(KernelFailure::InstanceFailed);
                }
                work.flush()?;
                if state.is_empty() {
                    builder.append_null();
                } else {
                    let mut buffer = state.buffer();
                    core::serialize_set_into(state, &mut buffer, work)
                        .map_err(core::DistinctComputationError::into_kernel_failure)?;
                    payload_bytes =
                        core::checked_state_batch_bytes(payload_bytes, buffer.bytes.len())
                            .map_err(core::DistinctComputationError::into_kernel_failure)?;
                    work.flush()?;
                    // Arrow output is an opaque host-scoped allocation, as in v1.
                    builder.append_value(buffer.bytes.as_slice());
                }
                work.flush()?;
                rows += 1;
                work.step()?;
            }
            if rows != expected {
                return Err(internal(
                    "numeric DISTINCT state iterator shortened its extent",
                ));
            }
            work.flush()?;
            let output = Arc::new(builder.finish());
            work.flush()?;
            Ok(output as ArrayRef)
        })
    }
    fn build_final<'s, I>(
        &self,
        mut states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'s Self::State>,
    {
        observed(control, |work| {
            let expected = states.len();
            let Some(first) = states.next() else {
                if expected != 0 {
                    return Err(internal(
                        "numeric DISTINCT state iterator shortened its extent",
                    ));
                }
                work.flush()?;
                let output = new_empty_array(self.output_type());
                work.flush()?;
                return Ok(output);
            };
            if expected == 0 {
                return Err(internal(
                    "numeric DISTINCT state iterator exceeded its extent",
                ));
            }
            let mut arrays = HostVec::new_in(first.allocator.clone());
            arrays.try_reserve_exact(expected).map_err(|source| {
                arrays
                    .allocator()
                    .scalar_allocation_error("reserve numeric distinct output arrays", source)
            })?;
            work.flush()?;
            for state in std::iter::once(first).chain(states) {
                if state.failed {
                    return Err(KernelFailure::InstanceFailed);
                }
                if arrays.len() == expected {
                    return Err(internal(
                        "numeric DISTINCT state iterator exceeded its extent",
                    ));
                }
                let value = match self.operation {
                    DistinctOperation::Sum => {
                        core::sum_from_set(state, self.source_type(), self.output_type(), work)
                    }
                    DistinctOperation::Avg => {
                        core::avg_from_set(state, self.source_type(), self.output_type(), work)
                    }
                }
                .map_err(core::DistinctComputationError::into_kernel_failure)?;
                arrays.push(value);
                work.step()?;
            }
            if arrays.len() != expected {
                return Err(internal(
                    "numeric DISTINCT state iterator shortened its extent",
                ));
            }
            let mut references = HostVec::new_in(first.allocator.clone());
            references.try_reserve_exact(expected).map_err(|source| {
                references
                    .allocator()
                    .scalar_allocation_error("reserve numeric distinct output references", source)
            })?;
            for array in &arrays {
                references.push(array.as_ref());
                work.step()?;
            }
            work.flush()?;
            let output = arrow_select::concat::concat(&references).map_err(|source| {
                core::DistinctComputationError::OutputConcat(source).into_kernel_failure()
            })?;
            work.flush()?;
            Ok(output)
        })
    }
}

pub(super) fn validate_contract(
    operation: DistinctOperation,
    contract: &AggregateCallContract,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    use novarocks_type_contract::ValueLogicalType;
    let selected = contract.call().selected();
    let [FunctionArgumentType::Value(source)] = selected.argument_types.as_ref() else {
        return Err(invalid(
            "numeric DISTINCT requires exactly one value argument",
        ));
    };
    let FunctionResultType::Scalar(result) = &selected.result_type else {
        return Err(invalid("numeric DISTINCT requires a scalar result"));
    };
    let expected = match (source.data_type(), operation) {
        (
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64,
            DistinctOperation::Avg,
        ) => DataType::Float64,
        (
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64,
            DistinctOperation::Sum,
        ) => DataType::Int64,
        (DataType::Float32 | DataType::Float64, DistinctOperation::Sum) => DataType::Float64,
        (ty @ DataType::Decimal128(..), op) => novarocks_type_contract::canonical_agg_decimal_type(
            if op == DistinctOperation::Avg {
                "avg"
            } else {
                "multi_distinct_sum"
            },
            ty,
        )
        .ok_or_else(|| invalid("numeric DISTINCT decimal output is unsupported"))?,
        (ty @ DataType::Decimal256(..), DistinctOperation::Avg) => ty.clone(),
        _ => {
            return Err(invalid(&format!(
                "{} has no installed numeric DISTINCT input profile for {:?}",
                contract.call().function_id().as_str(),
                source.data_type(),
            )));
        }
    };
    work.step()
        .map_err(crate::kernel_control::compile_failure)?;
    if source.logical_type() != ValueLogicalType::Physical
        || result.logical_type() != ValueLogicalType::Physical
        || result.data_type() != &expected
        || contract.intermediate_type().logical_type() != ValueLogicalType::Physical
        || contract.intermediate_type().data_type() != &DataType::Binary
    {
        return Err(invalid(
            "numeric DISTINCT selected input, result or state domain differs from its owner",
        ));
    }
    Ok(())
}
