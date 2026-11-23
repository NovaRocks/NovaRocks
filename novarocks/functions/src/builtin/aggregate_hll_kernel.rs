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
//! Exact NDV lifecycle over the original shared HLL register/state author.
use super::aggregate_hll_core::{self as core, HLL_REGISTERS_COUNT, HllRawState, HllWork};
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_invocation_backing::HostDiagnostic;
use crate::kernel_control::{KernelControlObservation, compile_failure, internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::*;
use arrow_array::builder::BinaryBuilder;
use arrow_array::{Array, ArrayRef, BinaryArray, Int64Array};
use arrow_schema::DataType;
use std::sync::Arc;
#[derive(Debug)]
pub(super) struct NdvKernel {
    pub(super) contract: Arc<AggregateCallContract>,
}
pub(super) struct NdvState {
    pub(super) core: HllRawState<HostAggregateAllocator>,
    pub(super) failed: bool,
}
impl NdvState {
    fn latch_failure(&mut self) {
        self.core.clear();
        self.failed = true;
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
pub(super) struct NdvUpdate<'a> {
    input: SelectedAggregateUpdateInput<'a, 'a>,
}
pub(super) struct NdvMerge<'a> {
    input: SelectedAggregateMergeInput<'a, 'a>,
    diagnostic: Option<(core::HllMergeFailure, InvocationData)>,
}
fn observed_evaluation<T>(
    control: &dyn KernelEvaluationControl,
    f: impl FnOnce(&mut EvaluationCheckpoints<'_>) -> Result<T, EvaluationFailure>,
) -> Result<T, EvaluationFailure> {
    let observation = KernelControlObservation::new(control);
    observation.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(&observation);
    let result = f(&mut work);
    match result {
        Err(data @ EvaluationFailure::InvocationData(_)) => Err(data),
        Ok(value) => observation
            .finish(work.finish_result(Ok(value)))
            .map_err(Into::into),
        Err(EvaluationFailure::Kernel(cause)) => observation
            .finish(work.finish_result(Err(cause)))
            .map_err(Into::into),
    }
}
impl PreparedAggregateKernel for NdvKernel {
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
    type State = NdvState;
    type PreparedUpdateBatch<'a> = NdvUpdate<'a>;
    fn has_invocation_data(&self) -> bool {
        true
    }
    type PreparedMergeBatch<'a> = NdvMerge<'a>;
    fn contract(&self) -> &Arc<AggregateCallContract> {
        &self.contract
    }
    fn memory_policy(&self) -> AggregateStateMemoryPolicy {
        AggregateStateMemoryPolicy::AllocationTracked
    }
    fn retained_bytes(&self, state: &Self::State) -> usize {
        state.core.allocator.metadata_bytes()
            + if state.core.registers.is_some() {
                HLL_REGISTERS_COUNT
            } else {
                0
            }
    }
    fn prepared_merge_retained_bytes(&self, prepared: &Self::PreparedMergeBatch<'_>) -> usize {
        prepared
            .diagnostic
            .as_ref()
            .map_or(0, |(_, data)| data.retained_bytes())
    }
    fn create_state(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        control.checkpoint(0)?;
        Err(invalid("allocation-tracked NDV requires a host allocator"))
    }
    fn create_state_with_allocator(
        &self,
        allocator: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        observed(control, |work| {
            let host = allocator
                .ok_or_else(|| invalid("allocation-tracked NDV requires a host allocator"))?;
            work.step()?;
            Ok(NdvState {
                core: HllRawState::new(HostAggregateAllocator::try_new(host)?),
                failed: false,
            })
        })
    }
    fn prepare_update<'a>(
        &'a self,
        input: SelectedAggregateUpdateInput<'a, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedUpdateBatch<'a>, KernelFailure> {
        observed(control, |work| {
            work.step()?;
            if !std::ptr::eq(input.contract(), self.contract.as_ref())
                || !self.contract.phase().consumes_logical_arguments()
                || input.logical_arguments().len() != 1
                || !input.order_arguments().is_empty()
            {
                return Err(invalid(
                    "NDV update differs from its exact phase or channels",
                ));
            }
            Ok(NdvUpdate { input })
        })
    }
    fn update_row<'a>(
        &self,
        _state: &mut Self::State,
        _input: &Self::PreparedUpdateBatch<'a>,
        _ordinal: usize,
        _control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        Err(invalid("NDV requires its invocation Data protocol"))
    }
    fn update_row_evaluation<'a>(
        &self,
        state: &mut Self::State,
        prepared: &Self::PreparedUpdateBatch<'a>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), EvaluationFailure> {
        if state.failed {
            return Err(KernelFailure::InstanceFailed.into());
        }
        let input = prepared.input;
        let result = observed_evaluation(control, |work| {
            let row = input
                .selection()
                .row(ordinal)
                .ok_or_else(|| invalid("NDV selected ordinal is out of bounds"))?;
            work.step()?;
            let argument = input.logical_arguments()[0];
            let address = argument.value_row(ordinal, row);
            let mut work = HllWork::new(Some(work));
            let hash = core::hash_array_value_for_hll_with_failure(
                argument.array(),
                address,
                &mut work,
                |failure| EvaluationFailure::Kernel(failure.into_kernel()),
                EvaluationFailure::Kernel,
            )?;
            if let Some(hash) = hash {
                state.core.has_value = true;
                core::update_state_register_from_hash(&mut state.core, hash, &mut work)?;
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
        input: SelectedAggregateMergeInput<'a, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedMergeBatch<'a>, KernelFailure> {
        observed(control, |work| {
            work.step()?;
            if !std::ptr::eq(input.contract(), self.contract.as_ref())
                || self.contract.phase().consumes_logical_arguments()
                || input.state().array().data_type() != &DataType::Binary
            {
                return Err(invalid(
                    "NDV merge differs from its exact phase or Binary state",
                ));
            }
            Ok(NdvMerge {
                input,
                diagnostic: None,
            })
        })
    }
    fn prepare_merge_evaluation<'a>(
        &'a self,
        input: SelectedAggregateMergeInput<'a, 'a>,
        mapping: &[usize],
        allocator: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedMergeBatch<'a>, EvaluationFailure> {
        let mut prepared = self.prepare_merge(input, control)?;
        let argument = input.state();
        let array = argument
            .array()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| invalid("NDV merge requires its exact Binary state carrier"))?;
        let mut work = EvaluationCheckpoints::new(control);
        let mut failure = None;
        for ordinal in 0..input.selection().len() {
            let row = input
                .selection()
                .row(ordinal)
                .ok_or_else(|| invalid("NDV merge ordinal is out of bounds"))?;
            let address = argument.value_row(ordinal, row);
            if address >= array.len() {
                return Err(internal("NDV merge address is out of bounds").into());
            }
            work.step()?;
            if !array.is_null(address) {
                // This is the same header validator the original CPU invokes;
                // sparse entries and prior state mutation are not previewed.
                failure = core::hll_payload_recipe(array.value(address)).err();
                if failure.is_some() {
                    break;
                }
            }
        }
        work.flush()?;
        let Some(failure) = failure else {
            return Ok(prepared);
        };
        let allocator = HostAggregateAllocator::try_new(
            allocator.ok_or_else(|| invalid("NDV invocation Data requires its host allocator"))?,
        )?;
        let diagnostic = HostDiagnostic::prepare(&allocator, &mut work, |writer| {
            write!(
                writer,
                "{}",
                crate::aggregate_format::AggregateFailureStage::Merge.message(failure.message())
            )
        })?;
        let data = InvocationData::prepare_aggregate_with_reason(
            &allocator,
            self.contract.clone(),
            AggregateInvocationPhase::Merge,
            input.selection(),
            mapping,
            diagnostic,
            crate::DataFailureReason::InvalidEncoding,
            &mut work,
        )?;
        prepared.diagnostic = Some((failure, data));
        work.finish()?;
        Ok(prepared)
    }
    fn merge_row<'a>(
        &self,
        _state: &mut Self::State,
        _input: &Self::PreparedMergeBatch<'a>,
        _ordinal: usize,
        _control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        Err(invalid("NDV requires its invocation Data protocol"))
    }
    fn merge_row_evaluation<'a>(
        &self,
        state: &mut Self::State,
        prepared: &Self::PreparedMergeBatch<'a>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), EvaluationFailure> {
        if state.failed {
            return Err(KernelFailure::InstanceFailed.into());
        }
        let input = prepared.input;
        let result = observed_evaluation(control, |work| {
            let row = input
                .selection()
                .row(ordinal)
                .ok_or_else(|| invalid("NDV merge ordinal is out of bounds"))?;
            work.step()?;
            let argument = input.state();
            let address = argument.value_row(ordinal, row);
            let array = argument
                .array()
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| invalid("NDV merge requires its exact Binary state carrier"))?;
            if address >= array.len() {
                return Err(internal("NDV merge address is out of bounds").into());
            }
            if !array.is_null(address) {
                state.core.has_value = true;
                core::merge_hll_bytes_with_failure(
                    &mut state.core,
                    array.value(address),
                    &mut HllWork::new(Some(work)),
                    |failure| match &prepared.diagnostic {
                        Some((expected, data)) if failure == *expected => {
                            EvaluationFailure::InvocationData(data.clone())
                        }
                        _ => internal(
                            "validated HLL payload violated its exact prepared failure recipe",
                        )
                        .into(),
                    },
                    EvaluationFailure::Kernel,
                )?;
            }
            Ok(())
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
            work.flush()?;
            let mut builder = BinaryBuilder::new();
            work.flush()?;
            let mut count = 0;
            for state in states {
                if count == expected {
                    return Err(internal("NDV state iterator exceeded its extent"));
                }
                if state.failed {
                    return Err(KernelFailure::InstanceFailed);
                }
                work.step()?;
                let payload =
                    core::serialize_hll_state(&state.core, &mut HllWork::new(Some(work)))?;
                match payload {
                    Some(payload) => {
                        work.flush()?;
                        builder.append_value(payload);
                        work.flush()?;
                    }
                    None => builder.append_null(),
                }
                count += 1;
            }
            if count != expected {
                return Err(internal("NDV state iterator shortened its extent"));
            }
            work.flush()?;
            let result = Arc::new(builder.finish()) as ArrayRef;
            work.flush()?;
            Ok(result)
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
            work.flush()?;
            let mut values = Vec::new();
            values
                .try_reserve_exact(expected)
                .map_err(|_| KernelFailure::ResourceExhausted)?;
            work.flush()?;
            for state in states {
                if values.len() == expected {
                    return Err(internal("NDV state iterator exceeded its extent"));
                }
                if state.failed {
                    return Err(KernelFailure::InstanceFailed);
                }
                work.step()?;
                values.push(core::estimate_cardinality(
                    &state.core,
                    &mut HllWork::new(Some(work)),
                )?);
            }
            if values.len() != expected {
                return Err(internal("NDV state iterator shortened its extent"));
            }
            work.flush()?;
            let result = Arc::new(Int64Array::from(values)) as ArrayRef;
            work.flush()?;
            Ok(result)
        })
    }
}
pub(super) fn validate_contract(
    contract: &AggregateCallContract,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    work.step().map_err(compile_failure)?;
    let [FunctionArgumentType::Value(_source)] = contract.call().selected().argument_types.as_ref()
    else {
        return Err(invalid("NDV requires exactly one value argument"));
    };
    let FunctionResultType::Scalar(output) = &contract.call().selected().result_type else {
        return Err(invalid("NDV requires a scalar result"));
    };
    // Generic binding is preserved: unsupported raw carriers fail in the original
    // selected hash reader only when actually demanded, rather than narrowing admission.
    for (actual, carrier) in [
        (output, DataType::Int64),
        (contract.intermediate_type(), DataType::Binary),
    ] {
        // Borrow the original canonical type recipe instead of publishing
        // two expected FVT/layout owners used only for this comparison.
        let matches = crate::aggregate_types::AggregateTypeRecipe::Scalar(carrier)
            .matches_value_observed(
                actual.typed_binding().borrowed(),
                true,
                novarocks_type_contract::ValueLogicalType::Physical,
                work,
            )
            .map_err(|error| match error {
                FunctionBindingError::Control(cause) => compile_failure(cause),
                _ => invalid("NDV canonical type comparison failed"),
            })?;
        if !matches {
            return Err(invalid(
                "NDV requires its exact nullable Int64 result and Binary state",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "aggregate_hll_invocation_data_tests.rs"]
mod tests;
