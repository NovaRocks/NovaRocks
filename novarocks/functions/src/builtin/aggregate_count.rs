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

//! Exact four-phase non-DISTINCT COUNT lifecycle. Inline state has no heap.
//! Allocation Layout checks describe requests; host memory grants remain external.

use super::aggregate_count_core::{self as core, CountObservation};
use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::{
    AggregateCallContract, AggregateStateMemoryPolicy, KernelEvaluationControl, KernelFailure,
    PreparedAggregateKernel, SelectedAggregateMergeInput, SelectedAggregateUpdateInput,
};
use arrow_array::{Array, ArrayRef, Int64Array};
use std::{alloc::Layout, sync::Arc};

#[derive(Debug)]
pub(super) struct CountKernel {
    pub(super) contract: Arc<AggregateCallContract>,
}

fn primary(error: &KernelFailure) -> bool {
    error.is_interruption()
}
fn observed<T>(
    control: &dyn KernelEvaluationControl,
    operation: impl FnOnce(&mut EvaluationCheckpoints<'_>) -> Result<T, KernelFailure>,
) -> Result<T, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = operation(&mut work);
    if result.as_ref().is_err_and(primary) {
        return result;
    }
    work.finish()?;
    result
}
fn increment(
    state: &mut i64,
    contribution: i64,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    core::add_observed(state, contribution, &mut |observation| match observation {
        CountObservation::Step => work.step(),
        CountObservation::OpaqueBoundary => work.flush(),
    })
}
fn output_capacity(rows: usize) -> Result<(), KernelFailure> {
    Layout::array::<i64>(rows).map_err(|_| KernelFailure::ResourceExhausted)?;
    Ok(())
}
impl PreparedAggregateKernel for CountKernel {
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
    type State = i64;
    type PreparedUpdateBatch<'batch> = SelectedAggregateUpdateInput<'batch, 'batch>;
    type PreparedMergeBatch<'batch> = SelectedAggregateMergeInput<'batch, 'batch>;
    fn contract(&self) -> &Arc<AggregateCallContract> {
        &self.contract
    }
    fn argument_encoding(&self, ordinal: usize) -> crate::AggregateArgumentEncoding<'_> {
        if self.contract.phase().consumes_logical_arguments()
            && self.contract.logical_argument_types().len() == 1
            && ordinal == 0
        {
            crate::AggregateArgumentEncoding::root_int32_string_dictionary(
                self.contract.as_ref(),
                ordinal,
            )
        } else {
            crate::AggregateArgumentEncoding::plain(self.contract.as_ref(), ordinal)
        }
    }
    fn memory_policy(&self) -> AggregateStateMemoryPolicy {
        AggregateStateMemoryPolicy::FixedZero
    }
    fn retained_bytes(&self, _: &i64) -> usize {
        0
    }
    fn create_state(&self, control: &dyn KernelEvaluationControl) -> Result<i64, KernelFailure> {
        observed(control, |work| {
            work.step()?;
            Ok(core::initial_state())
        })
    }
    fn prepare_update<'batch>(
        &'batch self,
        input: SelectedAggregateUpdateInput<'batch, 'batch>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedUpdateBatch<'batch>, KernelFailure> {
        observed(control, |work| {
            let valid = std::ptr::eq(input.contract(), self.contract.as_ref())
                && self.contract.phase().consumes_logical_arguments()
                && input.logical_arguments().len() == self.contract.call().logical_argument_count()
                && input.logical_arguments().len() <= 1
                && input.order_arguments().is_empty();
            work.step()?;
            if !valid {
                return Err(invalid(
                    "COUNT update differs from its exact phase or channels",
                ));
            }
            Ok(input)
        })
    }
    fn update_row<'batch>(
        &self,
        state: &mut i64,
        prepared: &Self::PreparedUpdateBatch<'batch>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        observed(control, |work| {
            let row = prepared.selection().row(ordinal);
            work.step()?;
            let row =
                row.ok_or_else(|| invalid("COUNT selected update ordinal is out of bounds"))?;
            if let Some(argument) = prepared.logical_arguments().first() {
                let address = argument.value_row(ordinal, row);
                let contributes = !crate::kernel_input::logical_is_null(
                    argument.array().as_ref(),
                    address,
                    1,
                    work,
                )?;
                work.step()?;
                if !contributes {
                    return Ok(());
                }
            }
            increment(state, 1, work)
        })
    }
    fn prepare_merge<'batch>(
        &'batch self,
        input: SelectedAggregateMergeInput<'batch, 'batch>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedMergeBatch<'batch>, KernelFailure> {
        observed(control, |work| {
            let valid = std::ptr::eq(input.contract(), self.contract.as_ref())
                && !self.contract.phase().consumes_logical_arguments();
            work.step()?;
            if !valid {
                return Err(invalid("COUNT merge differs from its exact phase"));
            }
            let concrete = input.state().array().as_any().is::<Int64Array>();
            work.step()?;
            if !concrete {
                return Err(internal("COUNT merge carrier is not an Int64Array"));
            }
            Ok(input)
        })
    }
    fn merge_row<'batch>(
        &self,
        state: &mut i64,
        prepared: &Self::PreparedMergeBatch<'batch>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        observed(control, |work| {
            let row = prepared.selection().row(ordinal);
            work.step()?;
            let row =
                row.ok_or_else(|| invalid("COUNT selected merge ordinal is out of bounds"))?;
            let argument = prepared.state();
            let address = argument.value_row(ordinal, row);
            let values = argument.array().as_any().downcast_ref::<Int64Array>();
            work.step()?;
            let values =
                values.ok_or_else(|| internal("COUNT merge carrier is not an Int64Array"))?;
            let valid = address < values.len();
            work.step()?;
            if !valid {
                return Err(internal("COUNT selected merge row is out of bounds"));
            }
            let null = values.is_null(address);
            work.step()?;
            if null {
                return Ok(());
            }
            let contribution = values.value(address);
            work.step()?;
            increment(state, contribution, work)
        })
    }
    fn build_intermediate<'state, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'state i64>,
    {
        build(states, control)
    }
    fn build_final<'state, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'state i64>,
    {
        build(states, control)
    }
}
fn build<'state, I: ExactSizeIterator<Item = &'state i64>>(
    states: I,
    control: &dyn KernelEvaluationControl,
) -> Result<ArrayRef, KernelFailure> {
    observed(control, |work| {
        let rows = states.len();
        output_capacity(rows)?;
        let emitted = std::cell::Cell::new(0usize);
        let values = states
            .map(|state| {
                let ordinal = emitted.get();
                if ordinal >= rows {
                    return Err(internal(
                        "COUNT emission iterator exceeds its admitted extent",
                    ));
                }
                emitted.set(ordinal + 1);
                Ok(*state)
            })
            .chain(
                std::iter::once_with(|| {
                    (emitted.get() != rows)
                        .then(|| Err(internal("COUNT emission iterator changed its exact extent")))
                })
                .flatten(),
            );
        core::build_state_array_observed(values, &mut |observation| match observation {
            CountObservation::Step => work.step(),
            CountObservation::OpaqueBoundary => work.flush(),
        })
    })
}

#[cfg(test)]
#[path = "aggregate_count_tests.rs"]
mod tests;
