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

//! GROUP_CONCAT over its original tracked row state and exact frozen parameters.
use super::aggregate_concat_core::{self as core, GroupConcatLayout, GroupConcatState};
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_scalar::ScalarWork;
use crate::kernel_control::{KernelControlObservation, compile_failure, internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::*;
use arrow_array::{Array, ArrayRef};
use arrow_schema::DataType;
use std::sync::Arc;
#[derive(Clone, Copy, Debug)]
pub(super) enum ConcatMode {
    Legacy { max_len: i64 },
    Listagg,
}

#[derive(Debug)]
pub(super) struct ConcatKernel {
    pub(super) contract: Arc<AggregateCallContract>,
    pub(super) ascending: Box<[bool]>,
    pub(super) nulls_first: Box<[bool]>,
    pub(super) mode: ConcatMode,
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
impl PreparedAggregateKernel for ConcatKernel {
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
            ascending: self.ascending.clone(),
            nulls_first: self.nulls_first.clone(),
            mode: self.mode,
        }))
    }
    type State = GroupConcatState<HostAggregateAllocator>;
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
            "allocation-tracked group_concat requires a host allocator",
        ))
    }
    fn create_state_with_allocator(
        &self,
        allocator: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        observed(control, |work| {
            let allocator = allocator.ok_or_else(|| {
                invalid("allocation-tracked group_concat requires a host allocator")
            })?;
            work.step()?;
            Ok(GroupConcatState::new(HostAggregateAllocator::try_new(
                allocator,
            )?))
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
                || input.logical_arguments().len() != self.contract.call().logical_argument_count()
                || input.order_arguments().len() != self.ascending.len()
            {
                return Err(invalid(
                    "group_concat update differs from its exact phase or channels",
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
                .ok_or_else(|| invalid("group_concat selected ordinal is out of bounds"))?;
            let mut addresses = Vec::new();
            addresses
                .try_reserve_exact(input.logical_arguments().len() + input.order_arguments().len())
                .map_err(|_| KernelFailure::ResourceExhausted)?;
            for argument in input
                .logical_arguments()
                .iter()
                .chain(input.order_arguments())
            {
                work.step()?;
                let address = argument.value_row(ordinal, row);
                if address >= argument.array().len() {
                    return Err(internal("group_concat selected address is out of bounds"));
                }
                addresses.push((argument.array(), address));
            }
            let layout = match self.mode {
                ConcatMode::Legacy { .. } => {
                    GroupConcatLayout::infer(addresses.len(), self.ascending.len())?
                }
                ConcatMode::Listagg => {
                    if addresses[..5].iter().any(|(array, index)| {
                        array.data_type() == &DataType::Null || array.is_null(*index)
                    }) {
                        return Ok(());
                    }
                    GroupConcatLayout {
                        output_col_num: 5,
                        separator_idx: None,
                        order_by_start: 5,
                        order_by_num: self.ascending.len(),
                    }
                }
            };
            state.update_row(&addresses, layout, &mut ScalarWork::new(Some(work)))
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
            {
                return Err(invalid(
                    "group_concat merge differs from its exact phase or channels",
                ));
            }
            core::GroupConcatMerge::new(
                input.state().array(),
                self.ascending.len(),
                &mut ScalarWork::new(Some(work)),
            )?;
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
                .ok_or_else(|| invalid("group_concat merge ordinal is out of bounds"))?;
            work.step()?;
            let argument = input.state();
            let address = argument.value_row(ordinal, row);
            if address >= argument.array().len() {
                return Err(internal("group_concat merge address is out of bounds"));
            }
            let mut scalar_work = ScalarWork::new(Some(work));
            let merge = core::GroupConcatMerge::new(
                argument.array(),
                self.ascending.len(),
                &mut scalar_work,
            )?;
            merge.merge_row(
                state,
                address,
                self.contract.intermediate_type().data_type(),
                &mut scalar_work,
            )
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
            let states = checked_states(states, work)?;
            core::build_intermediate_array(
                self.contract.intermediate_type().data_type(),
                states.iter().copied(),
                &mut ScalarWork::new(Some(work)),
            )
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
            let states = checked_states(states, work)?;
            let distinct = self
                .contract
                .state_interpretation()
                .expect("checked concatenation state interpretation")
                .distinct;
            let mut work = ScalarWork::new(Some(work));
            match self.mode {
                ConcatMode::Legacy { max_len } => core::build_final_array(
                    self.contract.intermediate_type().data_type(),
                    states.iter().copied(),
                    distinct,
                    &self.ascending,
                    &self.nulls_first,
                    max_len,
                    &mut work,
                ),
                ConcatMode::Listagg => core::build_listagg_array(
                    states.iter().copied(),
                    distinct,
                    &self.ascending,
                    &self.nulls_first,
                    &mut work,
                ),
            }
        })
    }
}
fn checked_states<'s, I: ExactSizeIterator<Item = &'s GroupConcatState<HostAggregateAllocator>>>(
    states: I,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Vec<&'s GroupConcatState<HostAggregateAllocator>>, KernelFailure> {
    let expected = states.len();
    let mut selected = Vec::new();
    selected
        .try_reserve_exact(expected)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    for state in states {
        work.step()?;
        if selected.len() == expected {
            return Err(internal("group_concat state iterator exceeded its extent"));
        }
        if state.failed {
            return Err(KernelFailure::InstanceFailed);
        }
        selected.push(state);
    }
    if selected.len() != expected {
        return Err(internal("group_concat state iterator shortened its extent"));
    }
    Ok(selected)
}
pub(super) fn validate_contract(
    contract: &AggregateCallContract,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    let count = contract.call().logical_argument_count();
    let arguments = &contract.call().selected().argument_types;
    if count < 2 || count > arguments.len() {
        return Err(invalid(
            "group_concat requires output columns and its canonical separator",
        ));
    }
    for (index, argument) in arguments.iter().enumerate() {
        work.step().map_err(compile_failure)?;
        let FunctionArgumentType::Value(value) = argument else {
            return Err(invalid("group_concat requires canonical value arguments"));
        };
        // The legacy scalar codec reads the exact carrier independently of
        // logical annotations; binding retains those annotations unchanged.
        if !super::aggregate_any_value::supported(value.data_type(), work)? {
            return Err(invalid(&format!(
                "group_concat has no installed scalar profile for {:?}",
                value.data_type()
            )));
        }
        if index == count - 1 && value.data_type() != &DataType::Utf8 {
            return Err(invalid(
                "group_concat requires the canonical UTF8 separator",
            ));
        }
    }
    // Exact selected field metadata is retained; only original codec shape is validated.
    let fields = core::validate_intermediate_type(contract.intermediate_type().data_type())
        .map_err(KernelFailure::from_domain)?;
    let mut actual_types =
        core::intermediate_arg_types_borrowed(contract.intermediate_type().data_type())
            .map_err(KernelFailure::from_domain)?;
    let exact_channels = if fields.len() != arguments.len() {
        false
    } else {
        let mut same = true;
        for (actual, argument) in actual_types.by_ref().zip(arguments.iter()) {
            let FunctionArgumentType::Value(expected) = argument else {
                unreachable!("checked value channels")
            };
            if !novarocks_type_contract::arrow_data_types_exact_observed::<KernelFailure>(
                actual,
                expected.data_type(),
                || work.step().map_err(compile_failure),
            )? {
                same = false;
                break;
            }
        }
        same
    };
    if !exact_channels
        || !contract.intermediate_type().nullable()
        || contract.final_type().data_type() != &DataType::Utf8
        || !contract.final_type().nullable()
    {
        return Err(invalid(
            "group_concat selected result or intermediate differs from its exact nullable UTF8 and STRUCT<ARRAY> state",
        ));
    }
    let original = contract
        .state_interpretation()
        .ok_or_else(|| invalid("group_concat requires its original state interpretation facts"))?;
    if original.order_keys.len() != arguments.len() - count {
        return Err(invalid(
            "group_concat state interpretation has a different channel shape",
        ));
    }
    Ok(())
}
#[cfg(test)]
#[path = "aggregate_concat_failure_tests.rs"]
mod failure_tests;
