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
use super::aggregate_concat_core::{self as core, ListaggState};
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_scalar::ScalarWork;
use crate::kernel_control::{KernelControlObservation, internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::*;
use arrow_array::{Array, ArrayRef};
use arrow_schema::DataType;
use std::sync::Arc;
#[derive(Debug)]
pub(super) struct ListaggKernel {
    pub(super) contract: Arc<AggregateCallContract>,
    pub(super) ascending: Box<[bool]>,
    pub(super) nulls_first: Box<[bool]>,
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
impl PreparedAggregateKernel for ListaggKernel {
    type State = ListaggState<HostAggregateAllocator>;
    type PreparedUpdateBatch<'a> = SelectedAggregateUpdateInput<'a, 'a>;
    type PreparedMergeBatch<'a> = ();
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
            "allocation-tracked LISTAGG requires a host allocator",
        ))
    }
    fn create_state_with_allocator(
        &self,
        allocator: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        observed(control, |work| {
            let allocator = allocator
                .ok_or_else(|| invalid("allocation-tracked LISTAGG requires a host allocator"))?;
            work.step()?;
            Ok(ListaggState::new(HostAggregateAllocator::try_new(
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
                    "LISTAGG update differs from its exact phase or channels",
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
                .ok_or_else(|| invalid("LISTAGG selected ordinal is out of bounds"))?;
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
                    return Err(internal("LISTAGG selected address is out of bounds"));
                }
                addresses.push((argument.array(), address));
            }
            if addresses[..5]
                .iter()
                .any(|(array, index)| array.data_type() == &DataType::Null || array.is_null(*index))
            {
                return Ok(());
            }
            state.update_row(&addresses, &mut ScalarWork::new(Some(work)))
        });
        if result.is_err() {
            state.latch_failure();
        }
        result
    }
    fn prepare_merge<'a>(
        &'a self,
        _: SelectedAggregateMergeInput<'a, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedMergeBatch<'a>, KernelFailure> {
        control.checkpoint(0)?;
        Err(invalid("LISTAGG has no merge phase"))
    }

    fn merge_row<'a>(
        &self,
        _: &mut Self::State,
        _: &Self::PreparedMergeBatch<'a>,
        _: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        control.checkpoint(0)?;
        Err(invalid("LISTAGG has no merge phase"))
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
        Err(invalid("LISTAGG has no intermediate emission"))
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
            let distinct = self.contract.distinct();
            let mut work = ScalarWork::new(Some(work));
            core::build_listagg_array(
                states.iter().copied(),
                distinct,
                &self.ascending,
                &self.nulls_first,
                &mut work,
            )
        })
    }
}
fn checked_states<'s, I: ExactSizeIterator<Item = &'s ListaggState<HostAggregateAllocator>>>(
    states: I,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Vec<&'s ListaggState<HostAggregateAllocator>>, KernelFailure> {
    let expected = states.len();
    let mut selected = Vec::new();
    selected
        .try_reserve_exact(expected)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    for state in states {
        work.step()?;
        if selected.len() == expected {
            return Err(internal("LISTAGG state iterator exceeded its extent"));
        }
        if state.failed {
            return Err(KernelFailure::InstanceFailed);
        }
        selected.push(state);
    }
    if selected.len() != expected {
        return Err(internal("LISTAGG state iterator shortened its extent"));
    }
    Ok(selected)
}
#[cfg(test)]
#[path = "aggregate_concat_failure_tests.rs"]
mod failure_tests;
