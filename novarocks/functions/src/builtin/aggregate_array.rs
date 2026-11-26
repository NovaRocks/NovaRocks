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

//! Selected ARRAY family through its original tracked rows and output codec.
use super::aggregate_array_core::{self as core, ArrayAggConfig, ArrayAggKind, ArrayAggState};
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_scalar::ScalarWork;
use crate::kernel_control::{KernelControlObservation, compile_failure, internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::*;
use arrow_array::{Array, ArrayRef};
use arrow_schema::DataType;
use novarocks_type_contract::{ValueLogicalType, ValueTypeRef};
use std::sync::Arc;
#[derive(Debug, Clone, Copy)]
pub(super) enum Operation {
    Array,
    Distinct,
    Unique,
}
#[derive(Debug)]
pub(super) struct ArrayKernel {
    pub(super) contract: Arc<AggregateCallContract>,
    pub(super) operation: Operation,
    pub(super) ascending: Box<[bool]>,
    pub(super) nulls_first: Box<[bool]>,
}
#[derive(Debug)]
pub(super) struct ArrayState {
    values: ArrayAggState<HostAggregateAllocator>,
    failed: bool,
}
impl ArrayState {
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
impl ArrayKernel {
    fn kind(&self) -> ArrayAggKind<'_> {
        match self.operation {
            Operation::Array => ArrayAggKind::Array {
                distinct: self
                    .contract
                    .state_interpretation()
                    .map_or_else(|| self.contract.distinct(), |original| original.distinct),
                ascending: &self.ascending,
                nulls_first: &self.nulls_first,
            },
            Operation::Distinct => ArrayAggKind::Array {
                distinct: true,
                ascending: &self.ascending,
                nulls_first: &self.nulls_first,
            },
            Operation::Unique => ArrayAggKind::Unique,
        }
    }
    fn config(&self) -> ArrayAggConfig<'_> {
        let DataType::List(item) = self.contract.final_type().data_type() else {
            unreachable!("validated ARRAY result")
        };
        ArrayAggConfig {
            kind: self.kind(),
            output_type: self.contract.final_type().data_type(),
            intermediate_type: self.contract.intermediate_type().data_type(),
            input_arg_type: Some(item.data_type()),
        }
    }
    fn output<'s, I: ExactSizeIterator<Item = &'s ArrayState>>(
        &self,
        states: I,
        intermediate: bool,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure> {
        observed(control, |work| {
            let mut checked = Vec::new();
            checked
                .try_reserve_exact(states.len())
                .map_err(|_| KernelFailure::ResourceExhausted)?;
            for state in states {
                work.step()?;
                if state.failed {
                    return Err(KernelFailure::InstanceFailed);
                }
                checked.push(&state.values);
            }
            core::build_array(
                &self.config(),
                checked.into_iter(),
                intermediate,
                &mut ScalarWork::new(Some(work)),
            )
        })
    }
}
impl PreparedAggregateKernel for ArrayKernel {
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
            ascending: self.ascending.clone(),
            nulls_first: self.nulls_first.clone(),
        }))
    }
    type State = ArrayState;
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
            "allocation-tracked ARRAY aggregate requires a host allocator",
        ))
    }
    fn create_state_with_allocator(
        &self,
        allocator: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        observed(control, |work| {
            let host = allocator.ok_or_else(|| {
                invalid("allocation-tracked ARRAY aggregate requires a host allocator")
            })?;
            work.step()?;
            Ok(ArrayState {
                values: ArrayAggState::new(HostAggregateAllocator::try_new(host)?),
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
            work.step()?;
            if !std::ptr::eq(input.contract(), self.contract.as_ref())
                || !self.contract.phase().consumes_logical_arguments()
                || input.logical_arguments().len() != 1
                || input.order_arguments().len() != self.ascending.len()
            {
                return Err(invalid(
                    "ARRAY aggregate update differs from its exact phase or channels",
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
                .ok_or_else(|| invalid("ARRAY aggregate selected ordinal is out of bounds"))?;
            for argument in input
                .logical_arguments()
                .iter()
                .chain(input.order_arguments())
            {
                work.step()?;
                if argument.value_row(ordinal, row) >= argument.array().len() {
                    return Err(internal(
                        "ARRAY aggregate selected address is out of bounds",
                    ));
                }
            }
            let logical = input.logical_arguments();
            let order = input.order_arguments();
            let addresses = (0..logical.len() + order.len()).map(|index| {
                let argument = if index < logical.len() {
                    &logical[index]
                } else {
                    &order[index - logical.len()]
                };
                (argument.array(), argument.value_row(ordinal, row))
            });
            core::update_row(
                &mut state.values,
                self.kind(),
                addresses,
                !input.order_arguments().is_empty(),
                false,
                &mut ScalarWork::new(Some(work)),
            )
        });
        if result.is_err() {
            state.latch_failure()
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
                    "ARRAY aggregate merge differs from its exact phase or channels",
                ));
            }
            core::merge_input_list_array(input.state().array())
                .map_err(KernelFailure::from_domain)?;
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
                .ok_or_else(|| invalid("ARRAY aggregate merge ordinal is out of bounds"))?;
            work.step()?;
            let argument = input.state();
            let address = argument.value_row(ordinal, row);
            if address >= argument.array().len() {
                return Err(internal("ARRAY aggregate merge address is out of bounds"));
            }
            core::merge_row(
                &mut state.values,
                self.kind(),
                argument.array(),
                address,
                &mut ScalarWork::new(Some(work)),
            )
        });
        if result.is_err() {
            state.latch_failure()
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
        self.output(states, true, control)
    }
    fn build_final<'s, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'s Self::State>,
    {
        self.output(states, false, control)
    }
}
fn supported(
    ty: &DataType,
    json_payload: Option<&ValueTypeRef>,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<bool, KernelFailure> {
    work.step().map_err(compile_failure)?;
    let child = |index| json_payload.and_then(|value| value.child_type_at(index));
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
        | DataType::Date32
        | DataType::Timestamp(..)
        | DataType::Decimal128(..) => true,
        DataType::LargeBinary => {
            json_payload.and_then(ValueTypeRef::domain) == Some(ValueLogicalType::Json)
        }
        DataType::FixedSizeBinary(16) => true,
        DataType::List(item) => supported(item.data_type(), child(0), work)?,
        DataType::Struct(fields) => {
            let mut all = !fields.is_empty();
            for (index, field) in fields.iter().enumerate() {
                all &= supported(field.data_type(), child(index), work)?;
            }
            all
        }
        DataType::Map(entries, _) => {
            if let DataType::Struct(fields) = entries.data_type() {
                fields.len() == 2
                    && supported(fields[0].data_type(), child(0), work)?
                    && supported(fields[1].data_type(), child(1), work)?
            } else {
                false
            }
        }
        _ => false,
    })
}
pub(super) fn validate_contract(
    operation: Operation,
    contract: &AggregateCallContract,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    let arguments = &contract.call().selected().argument_types;
    if contract.call().logical_argument_count() != 1 || arguments.is_empty() {
        return Err(invalid(
            "ARRAY aggregate requires exactly one logical value channel",
        ));
    }
    let receipt = contract.state_interpretation().ok_or_else(|| {
        invalid("ARRAY aggregate requires its original state interpretation facts")
    })?;
    if receipt.order_keys.len() != arguments.len() - 1 {
        return Err(invalid(
            "ARRAY aggregate state interpretation has a different channel shape",
        ));
    }
    if matches!(operation, Operation::Unique) && !receipt.order_keys.is_empty() {
        return Err(invalid(
            "array_unique_agg has no installed ordered state profile",
        ));
    }
    for (index, arg) in arguments.iter().enumerate() {
        let FunctionArgumentType::Value(value) = arg else {
            return Err(invalid("ARRAY aggregate requires canonical value channels"));
        };
        // The value channel may carry JSON bytes across partial and final
        // aggregation. Order and distinct still require their existing domains.
        let json_payload =
            (index == 0 && matches!(operation, Operation::Array) && !receipt.distinct)
                .then_some(value.value_type());
        if !supported(value.data_type(), json_payload, work)? {
            return Err(invalid(&format!(
                "{} has no installed ARRAY input profile for {:?}",
                contract.call().function_id().as_str(),
                value.data_type()
            )));
        }
    }
    let FunctionArgumentType::Value(value) = &arguments[0] else {
        unreachable!("validated value")
    };
    if matches!(operation, Operation::Unique) && !matches!(value.data_type(), DataType::List(_)) {
        return Err(invalid(
            "array_unique_agg has no installed non-List input profile",
        ));
    }
    let DataType::List(item) = contract.final_type().data_type() else {
        return Err(invalid(
            "ARRAY aggregate selected result must be its exact nullable List profile",
        ));
    };
    let expected_item = if matches!(operation, Operation::Unique) {
        let DataType::List(source_item) = value.data_type() else {
            return Err(invalid(
                "array_unique_agg has no installed non-List input profile",
            ));
        };
        source_item.data_type()
    } else {
        value.data_type()
    };
    if item.data_type() != expected_item
        || !contract.final_type().nullable()
        || !contract.intermediate_type().nullable()
    {
        return Err(invalid(
            "ARRAY aggregate selected result or state differs from its exact nullable profile",
        ));
    }
    let intermediate = contract.intermediate_type().data_type();
    if arguments.len() == 1 {
        let DataType::List(state_item) = intermediate else {
            return Err(invalid("ARRAY aggregate plain state must be List"));
        };
        if state_item.data_type() != expected_item {
            return Err(invalid(
                "ARRAY aggregate state item differs from its original source",
            ));
        }
    } else {
        let DataType::Struct(fields) = intermediate else {
            return Err(invalid(
                "ARRAY aggregate ordered state must be Struct<List>",
            ));
        };
        if fields.len() != arguments.len() {
            return Err(invalid(
                "ARRAY aggregate ordered state has a different channel shape",
            ));
        }
        for (field, arg) in fields.iter().zip(arguments) {
            work.step().map_err(compile_failure)?;
            let (DataType::List(item), FunctionArgumentType::Value(value)) =
                (field.data_type(), arg)
            else {
                return Err(invalid(
                    "ARRAY aggregate ordered state channel must be List",
                ));
            };
            if item.data_type() != value.data_type() {
                return Err(invalid(
                    "ARRAY aggregate ordered state channel differs from its original source",
                ));
            }
        }
    }
    validate_full_selected_types(
        operation,
        arguments,
        contract.final_type(),
        contract.intermediate_type(),
        work,
    )?;
    Ok(())
}
fn validate_full_selected_types(
    operation: Operation,
    arguments: &[FunctionArgumentType],
    output: &FunctionValueType,
    state: &FunctionValueType,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    let name = match operation {
        Operation::Array => "array_agg",
        Operation::Distinct => "array_agg_distinct",
        Operation::Unique => "array_unique_agg",
    };
    let valid = crate::aggregate_types::matches_selected_array_types_observed(
        name, arguments, output, state, work,
    )
    .map_err(|error| match error {
        FunctionBindingError::Control(cause) => compile_failure(cause),
        _ => invalid("ARRAY aggregate original full signature inference failed"),
    })?;
    if !valid {
        return Err(invalid(
            "ARRAY aggregate selected output or state differs from its original full signature",
        ));
    }
    Ok(())
}
#[cfg(test)]
#[path = "aggregate_array_failure_tests.rs"]
mod failure_tests;
