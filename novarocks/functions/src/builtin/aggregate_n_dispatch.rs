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

//! One owner dispatch, with disjoint prepared frames and states per profile.
use super::aggregate_n::NKernel as PhysicalKernel;
use super::aggregate_n_core::NState as PhysicalState;
use super::aggregate_n_largeint::{
    NLargeIntKernel, NLargeIntMerge, NLargeIntState, NLargeIntUpdate,
};
use super::aggregate_n_largeint_emission::NEmissionAccess;
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_scalar::ScalarStateError;
use crate::kernel_control::invalid;
use crate::*;
use arrow_array::ArrayRef;
use novarocks_type_contract::PureCompileControl;
use std::sync::Arc;

#[derive(Debug)]
pub(super) enum NKernel {
    Physical(Arc<PhysicalKernel>),
    LargeInt(Arc<NLargeIntKernel>),
}
pub(super) enum NProfileState {
    Physical(PhysicalState<HostAggregateAllocator>),
    LargeInt(NLargeIntState),
}
pub(super) enum NUpdate<'a> {
    Physical(SelectedAggregateUpdateInput<'a, 'a>),
    LargeInt(NLargeIntUpdate<'a>),
}
pub(super) enum NMerge<'a> {
    Physical(SelectedAggregateMergeInput<'a, 'a>),
    LargeInt(NLargeIntMerge<'a>),
}
fn mismatch() -> KernelFailure {
    invalid("min_n/max_n state or prepared frame belongs to another exact profile")
}
impl NEmissionAccess for NProfileState {
    fn latch(&self) {
        if let Self::LargeInt(state) = self {
            NEmissionAccess::latch(state);
        }
    }
    fn validate(
        &self,
        contract: &Arc<AggregateCallContract>,
        host: &Arc<dyn AggregateStateAllocator>,
    ) -> Result<(), KernelFailure> {
        match self {
            Self::LargeInt(state) => state.validate(contract, host),
            Self::Physical(_) => Err(mismatch()),
        }
    }
    fn with_core(
        &self,
        operation: impl FnOnce(&PhysicalState<HostAggregateAllocator>) -> Result<(), ScalarStateError>,
    ) -> Result<(), ScalarStateError> {
        match self {
            Self::LargeInt(state) => state.with_core(operation),
            Self::Physical(_) => Err(mismatch().into()),
        }
    }
}
fn physical_state(
    state: &NProfileState,
) -> Result<&PhysicalState<HostAggregateAllocator>, KernelFailure> {
    match state {
        NProfileState::Physical(state) => Ok(state),
        NProfileState::LargeInt(_) => Err(mismatch()),
    }
}
impl PreparedAggregateKernel for NKernel {
    type State = NProfileState;
    type PreparedUpdateBatch<'a> = NUpdate<'a>;
    type PreparedMergeBatch<'a> = NMerge<'a>;
    fn clone_for_local_phase(
        &self,
        contract: Arc<AggregateCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<Self>, KernelFailure> {
        Ok(Arc::new(match self {
            Self::Physical(kernel) => {
                Self::Physical(kernel.clone_for_local_phase(contract, control)?)
            }
            Self::LargeInt(kernel) => {
                Self::LargeInt(kernel.clone_for_local_phase(contract, control)?)
            }
        }))
    }
    fn contract(&self) -> &Arc<AggregateCallContract> {
        match self {
            Self::Physical(k) => k.contract(),
            Self::LargeInt(k) => k.contract(),
        }
    }
    fn memory_policy(&self) -> AggregateStateMemoryPolicy {
        AggregateStateMemoryPolicy::AllocationTracked
    }
    fn retained_bytes(&self, state: &Self::State) -> usize {
        // Retained stock belongs to the actual state, even when a caller passes
        // it to an incompatible kernel. Execution rejects that mix separately.
        match state {
            NProfileState::Physical(state) => {
                state.allocator.metadata_bytes() + state.retained_bytes()
            }
            NProfileState::LargeInt(state) => state.retained_bytes(),
        }
    }
    fn has_invocation_data(&self) -> bool {
        match self {
            Self::Physical(k) => k.has_invocation_data(),
            Self::LargeInt(k) => k.has_invocation_data(),
        }
    }
    fn requires_empty_update_preparation(&self) -> bool {
        match self {
            Self::Physical(k) => k.requires_empty_update_preparation(),
            Self::LargeInt(k) => k.requires_empty_update_preparation(),
        }
    }
    fn requires_emission_context(&self) -> bool {
        match self {
            Self::Physical(k) => k.requires_emission_context(),
            Self::LargeInt(k) => k.requires_emission_context(),
        }
    }
    fn requires_helper_memory(&self) -> bool {
        match self {
            Self::Physical(k) => k.requires_helper_memory(),
            Self::LargeInt(k) => k.requires_helper_memory(),
        }
    }
    fn create_state(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        match self {
            Self::Physical(k) => k.create_state(control).map(NProfileState::Physical),
            Self::LargeInt(k) => k.create_state(control).map(NProfileState::LargeInt),
        }
    }
    fn create_state_with_allocator(
        &self,
        host: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        match self {
            Self::Physical(k) => k
                .create_state_with_allocator(host, control)
                .map(NProfileState::Physical),
            Self::LargeInt(k) => k
                .create_state_with_allocator(host, control)
                .map(NProfileState::LargeInt),
        }
    }
    fn prepare_update<'a>(
        &'a self,
        input: SelectedAggregateUpdateInput<'a, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedUpdateBatch<'a>, KernelFailure> {
        match self {
            Self::Physical(k) => k.prepare_update(input, control).map(NUpdate::Physical),
            Self::LargeInt(k) => k.prepare_update(input, control).map(NUpdate::LargeInt),
        }
    }
    fn prepare_update_evaluation<'a>(
        &'a self,
        input: SelectedAggregateUpdateInput<'a, 'a>,
        mapping: &[usize],
        host: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedUpdateBatch<'a>, EvaluationFailure> {
        match self {
            Self::Physical(k) => k
                .prepare_update_evaluation(input, mapping, host, control)
                .map(NUpdate::Physical),
            Self::LargeInt(k) => k
                .prepare_update_evaluation(input, mapping, host, control)
                .map(NUpdate::LargeInt),
        }
    }
    fn prepared_update_retained_bytes(&self, batch: &Self::PreparedUpdateBatch<'_>) -> usize {
        match batch {
            NUpdate::Physical(_) => 0,
            NUpdate::LargeInt(p) => p.retained_bytes(),
        }
    }
    fn update_row<'a>(
        &self,
        state: &mut Self::State,
        batch: &Self::PreparedUpdateBatch<'a>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        match (self, state, batch) {
            (Self::Physical(k), NProfileState::Physical(state), NUpdate::Physical(p)) => {
                k.update_row(state, p, ordinal, control)
            }
            (Self::LargeInt(k), NProfileState::LargeInt(state), NUpdate::LargeInt(p)) => {
                k.update_row(state, p, ordinal, control)
            }
            _ => Err(mismatch().into()),
        }
    }
    fn update_row_evaluation<'a>(
        &self,
        state: &mut Self::State,
        batch: &Self::PreparedUpdateBatch<'a>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), EvaluationFailure> {
        match (self, state, batch) {
            (Self::Physical(k), NProfileState::Physical(state), NUpdate::Physical(p)) => {
                k.update_row_evaluation(state, p, ordinal, control)
            }
            (Self::LargeInt(k), NProfileState::LargeInt(state), NUpdate::LargeInt(p)) => {
                k.update_row_evaluation(state, p, ordinal, control)
            }
            _ => Err(mismatch().into()),
        }
    }
    fn prepare_merge<'a>(
        &'a self,
        input: SelectedAggregateMergeInput<'a, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedMergeBatch<'a>, KernelFailure> {
        match self {
            Self::Physical(k) => k.prepare_merge(input, control).map(NMerge::Physical),
            Self::LargeInt(k) => k.prepare_merge(input, control).map(NMerge::LargeInt),
        }
    }
    fn prepare_merge_evaluation<'a>(
        &'a self,
        input: SelectedAggregateMergeInput<'a, 'a>,
        mapping: &[usize],
        host: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedMergeBatch<'a>, EvaluationFailure> {
        match self {
            Self::Physical(k) => k
                .prepare_merge_evaluation(input, mapping, host, control)
                .map(NMerge::Physical),
            Self::LargeInt(k) => k
                .prepare_merge_evaluation(input, mapping, host, control)
                .map(NMerge::LargeInt),
        }
    }
    fn prepared_merge_retained_bytes(&self, batch: &Self::PreparedMergeBatch<'_>) -> usize {
        match batch {
            NMerge::Physical(_) => 0,
            NMerge::LargeInt(p) => p.retained_bytes(),
        }
    }
    fn merge_row<'a>(
        &self,
        state: &mut Self::State,
        batch: &Self::PreparedMergeBatch<'a>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        match (self, state, batch) {
            (Self::Physical(k), NProfileState::Physical(state), NMerge::Physical(p)) => {
                k.merge_row(state, p, ordinal, control)
            }
            (Self::LargeInt(k), NProfileState::LargeInt(state), NMerge::LargeInt(p)) => {
                k.merge_row(state, p, ordinal, control)
            }
            _ => Err(mismatch().into()),
        }
    }
    fn merge_row_evaluation<'a>(
        &self,
        state: &mut Self::State,
        batch: &Self::PreparedMergeBatch<'a>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), EvaluationFailure> {
        match (self, state, batch) {
            (Self::Physical(k), NProfileState::Physical(state), NMerge::Physical(p)) => {
                k.merge_row_evaluation(state, p, ordinal, control)
            }
            (Self::LargeInt(k), NProfileState::LargeInt(state), NMerge::LargeInt(p)) => {
                k.merge_row_evaluation(state, p, ordinal, control)
            }
            _ => Err(mismatch().into()),
        }
    }
    fn build_intermediate<'s, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'s Self::State>,
    {
        match self {
            Self::Physical(k) => k.build_intermediate_checked(states.map(physical_state), control),
            Self::LargeInt(_) => Err(invalid(
                "min_n/max_n LARGEINT requires its actual emission context",
            )),
        }
    }
    fn build_intermediate_evaluation_with_context<'s, I>(
        &self,
        states: I,
        context: &AggregateEmissionContext<'_>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, EvaluationFailure>
    where
        I: ExactSizeIterator<Item = &'s Self::State> + Clone,
    {
        match self {
            Self::Physical(k) => k
                .build_intermediate_checked(states.map(physical_state), control)
                .map_err(Into::into),
            Self::LargeInt(k) => k.emit(
                states,
                AggregateInvocationPhase::Intermediate,
                context,
                control,
            ),
        }
    }
    fn build_final<'s, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'s Self::State>,
    {
        match self {
            Self::Physical(k) => k.build_final_checked(states.map(physical_state), control),
            Self::LargeInt(_) => Err(invalid(
                "min_n/max_n LARGEINT requires its actual emission context",
            )),
        }
    }
    fn build_final_evaluation_with_context<'s, I>(
        &self,
        states: I,
        context: &AggregateEmissionContext<'_>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, EvaluationFailure>
    where
        I: ExactSizeIterator<Item = &'s Self::State> + Clone,
    {
        match self {
            Self::Physical(k) => k
                .build_final_checked(states.map(physical_state), control)
                .map_err(Into::into),
            Self::LargeInt(k) => k.emit(states, AggregateInvocationPhase::Final, context, control),
        }
    }
}
