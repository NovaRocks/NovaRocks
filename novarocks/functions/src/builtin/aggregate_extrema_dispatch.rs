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

//! One installed MIN/MAX owner dispatches exact fixed and variable-width lifecycle kernels.
//! The enum is an implementation detail, not a second signature or state arena.

use super::aggregate_extrema::{ExtremaKernel, ExtremaValue};
use super::aggregate_extrema_utf8::{BytesExtremaKernel, BytesExtremaState};
use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::{
    AggregateCallContract, AggregateStateMemoryPolicy, KernelEvaluationControl, KernelFailure,
    PreparedAggregateKernel, SelectedAggregateMergeInput, SelectedAggregateUpdateInput,
};
use arrow_array::ArrayRef;
use std::{alloc::Layout, sync::Arc};

#[derive(Debug)]
pub(super) enum PreparedExtrema {
    Fixed(ExtremaKernel),
    Bytes(BytesExtremaKernel),
}

pub(super) enum ExtremaState {
    Fixed(Option<ExtremaValue>),
    Bytes(BytesExtremaState),
}

fn wrong_state(control: &dyn KernelEvaluationControl) -> Result<(), KernelFailure> {
    control.checkpoint(0)?;
    control.checkpoint(1)?;
    Err(invalid(
        "MIN/MAX private state differs from its prepared carrier",
    ))
}

// These are temporary borrowed state references, never another owned aggregate
// arena. The host owns their source invoice and temporary-peak authorization.
fn borrowed_states<'a, S: 'a, I>(
    states: I,
    control: &dyn KernelEvaluationControl,
    project: impl Fn(&'a ExtremaState) -> Option<&'a S>,
) -> Result<Vec<&'a S>, KernelFailure>
where
    I: ExactSizeIterator<Item = &'a ExtremaState>,
{
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let expected = states.len();
        Layout::array::<&S>(expected).map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        let mut borrowed = Vec::new();
        borrowed
            .try_reserve_exact(expected)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        for state in states {
            work.step()?;
            if borrowed.len() == expected {
                return Err(internal(
                    "MIN/MAX state iterator exceeded its declared length",
                ));
            }
            let state = project(state)
                .ok_or_else(|| invalid("MIN/MAX output state differs from its prepared carrier"))?;
            borrowed.push(state);
        }
        let exact = borrowed.len() == expected;
        work.step()?;
        if !exact {
            return Err(internal(
                "MIN/MAX state iterator shortened its declared length",
            ));
        }
        Ok(borrowed)
    })();
    if result.as_ref().is_err_and(KernelFailure::is_interruption) {
        return result;
    }
    work.finish()?;
    result
}

impl PreparedAggregateKernel for PreparedExtrema {
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
        Ok(Arc::new(match self {
            Self::Fixed(kernel) => Self::Fixed(ExtremaKernel {
                contract,
                operation: kernel.operation,
            }),
            Self::Bytes(kernel) => Self::Bytes(BytesExtremaKernel {
                contract,
                operation: kernel.operation,
            }),
        }))
    }
    type State = ExtremaState;
    type PreparedUpdateBatch<'a> = SelectedAggregateUpdateInput<'a, 'a>;
    type PreparedMergeBatch<'a> = SelectedAggregateMergeInput<'a, 'a>;
    fn contract(&self) -> &Arc<AggregateCallContract> {
        match self {
            Self::Fixed(k) => k.contract(),
            Self::Bytes(k) => k.contract(),
        }
    }
    fn memory_policy(&self) -> AggregateStateMemoryPolicy {
        match self {
            Self::Fixed(k) => k.memory_policy(),
            Self::Bytes(k) => k.memory_policy(),
        }
    }
    fn create_state(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        match self {
            Self::Fixed(k) => k.create_state(control).map(ExtremaState::Fixed),
            Self::Bytes(k) => k.create_state(control).map(ExtremaState::Bytes),
        }
    }
    fn prepare_update<'a>(
        &'a self,
        input: Self::PreparedUpdateBatch<'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedUpdateBatch<'a>, KernelFailure> {
        match self {
            Self::Fixed(k) => k.prepare_update(input, control),
            Self::Bytes(k) => k.prepare_update(input, control),
        }
    }
    fn prepare_merge<'a>(
        &'a self,
        input: Self::PreparedMergeBatch<'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedMergeBatch<'a>, KernelFailure> {
        match self {
            Self::Fixed(k) => k.prepare_merge(input, control),
            Self::Bytes(k) => k.prepare_merge(input, control),
        }
    }
    fn update_row<'a>(
        &self,
        state: &mut Self::State,
        input: &Self::PreparedUpdateBatch<'a>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        match (self, state) {
            (Self::Fixed(k), ExtremaState::Fixed(state)) => {
                k.update_row(state, input, ordinal, control)
            }
            (Self::Bytes(k), ExtremaState::Bytes(state)) => {
                k.update_row(state, input, ordinal, control)
            }
            _ => wrong_state(control),
        }
    }
    fn merge_row<'a>(
        &self,
        state: &mut Self::State,
        input: &Self::PreparedMergeBatch<'a>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        match (self, state) {
            (Self::Fixed(k), ExtremaState::Fixed(state)) => {
                k.merge_row(state, input, ordinal, control)
            }
            (Self::Bytes(k), ExtremaState::Bytes(state)) => {
                k.merge_row(state, input, ordinal, control)
            }
            _ => wrong_state(control),
        }
    }
    fn build_intermediate<'a, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        Self::State: 'a,
        I: ExactSizeIterator<Item = &'a Self::State>,
    {
        match self {
            Self::Fixed(k) => {
                let states = borrowed_states(states, control, |s| match s {
                    ExtremaState::Fixed(s) => Some(s),
                    _ => None,
                })?;
                k.build_intermediate(states.into_iter(), control)
            }
            Self::Bytes(k) => {
                let states = borrowed_states(states, control, |s| match s {
                    ExtremaState::Bytes(s) => Some(s),
                    _ => None,
                })?;
                k.build_intermediate(states.into_iter(), control)
            }
        }
    }
    fn build_final<'a, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        Self::State: 'a,
        I: ExactSizeIterator<Item = &'a Self::State>,
    {
        match self {
            Self::Fixed(k) => {
                let states = borrowed_states(states, control, |s| match s {
                    ExtremaState::Fixed(s) => Some(s),
                    _ => None,
                })?;
                k.build_final(states.into_iter(), control)
            }
            Self::Bytes(k) => {
                let states = borrowed_states(states, control, |s| match s {
                    ExtremaState::Bytes(s) => Some(s),
                    _ => None,
                })?;
                k.build_final(states.into_iter(), control)
            }
        }
    }
    fn retained_bytes(&self, state: &Self::State) -> usize {
        // Count actual owned heap independently of the prepared discriminant.
        // Mutation and emission explicitly reject a mismatched private state.
        match state {
            ExtremaState::Fixed(_) => 0,
            ExtremaState::Bytes(state) => state.retained_bytes(),
        }
    }
}
