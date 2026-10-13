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

//! Private, unregistered selected consumer of the original Mann computation.
use super::aggregate_mann_whitney_core::{MannStateExtents, MannWhitneyInput, MannWhitneyState};
use super::aggregate_mann_whitney_emission::{MannEmissionAccess, emit_mann};
use super::aggregate_mann_whitney_operation::{MannDataDomain, call_input, call_unit};
use super::aggregate_mann_whitney_resources::MannSourceParams;
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_scalar::{ScalarStateError, ScalarWork};
use crate::kernel_control::{compile_failure, internal, invalid};
use crate::opaque_memory::OpaqueRetainedCharge;
use crate::scalar_resources::ScalarResourceError;
use crate::*;
use allocator_api2::vec::Vec as HostVec;
use arrow_array::{Array, ArrayRef, BinaryArray};
use arrow_schema::DataType;
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};
use std::{
    cell::{Cell, RefCell},
    sync::Arc,
};

fn resource_failure(cause: ScalarResourceError) -> KernelFailure {
    match cause {
        ScalarResourceError::Arithmetic => KernelFailure::ResourceExhausted,
        ScalarResourceError::SourceModel(detail) => invalid(detail),
        ScalarResourceError::Invariant(detail) => internal(detail),
    }
}
fn add(a: usize, b: usize) -> Result<usize, KernelFailure> {
    a.checked_add(b).ok_or(KernelFailure::ResourceExhausted)
}
fn allocator_retained(allocator: &HostAggregateAllocator) -> Result<usize, KernelFailure> {
    add(
        allocator.metadata_bytes(),
        allocator.journal_backing_bytes(),
    )
}

pub(super) struct MannState {
    core: RefCell<MannWhitneyState<HostAggregateAllocator>>,
    failed: Cell<bool>,
    base_retained: usize,
    retained: Cell<usize>,
}
impl MannState {
    fn account(
        &self,
        core: &MannWhitneyState<HostAggregateAllocator>,
    ) -> Result<(), KernelFailure> {
        let extents = core.extents();
        let count = add(extents.sample_capacities[0], extents.sample_capacities[1])?;
        let bytes = count
            .checked_mul(size_of::<f64>())
            .ok_or(KernelFailure::ResourceExhausted)?;
        self.retained.set(add(self.base_retained, bytes)?);
        Ok(())
    }
    fn latch(&self) {
        self.failed.set(true);
        // A conflicting borrow remains physically owned until its holder/State
        // exits. Never claim that inaccessible samples have already been freed.
        if let Ok(mut core) = self.core.try_borrow_mut() {
            let allocator = core.allocator.clone();
            *core = MannWhitneyState::new(allocator);
            self.retained.set(self.base_retained);
        }
    }
}
impl MannEmissionAccess for MannState {
    fn validate_host(&self, host: &Arc<dyn AggregateStateAllocator>) -> Result<(), KernelFailure> {
        if self.failed.get() {
            return Err(KernelFailure::InstanceFailed);
        }
        let core = self
            .core
            .try_borrow()
            .map_err(|_| internal("Mann-Whitney state has a conflicting emission borrow"))?;
        if !core.allocator.has_host_authority(host) {
            return Err(invalid(
                "Mann-Whitney emission uses another state host authority",
            ));
        }
        Ok(())
    }

    fn extents(&self) -> Result<MannStateExtents, KernelFailure> {
        if self.failed.get() {
            return Err(KernelFailure::InstanceFailed);
        }
        let core = self
            .core
            .try_borrow()
            .map_err(|_| internal("Mann-Whitney state has a conflicting emission borrow"))?;
        Ok(core.extents())
    }
    fn with_core(
        &self,
        operation: impl FnOnce(
            &mut MannWhitneyState<HostAggregateAllocator>,
        ) -> Result<(), ScalarStateError>,
    ) -> Result<(), ScalarStateError> {
        if self.failed.get() {
            return Err(KernelFailure::InstanceFailed.into());
        }
        let mut core = self
            .core
            .try_borrow_mut()
            .map_err(|_| internal("Mann-Whitney state has a conflicting emission borrow"))?;
        operation(&mut core)
    }
}

pub(super) struct MannUpdate<'a> {
    input: SelectedAggregateUpdateInput<'a, 'a>,
    view: MannWhitneyInput<'a>,
    mapping: HostVec<usize, HostAggregateAllocator>,
    allocator: HostAggregateAllocator,
    host: Arc<dyn AggregateStateAllocator>,
    charge: OpaqueRetainedCharge,
    retained: usize,
}
pub(super) struct MannMerge<'a> {
    input: SelectedAggregateMergeInput<'a, 'a>,
    mapping: HostVec<usize, HostAggregateAllocator>,
    allocator: HostAggregateAllocator,
    host: Arc<dyn AggregateStateAllocator>,
    charge: OpaqueRetainedCharge,
    retained: usize,
}
#[derive(Debug)]
pub(super) struct MannKernel {
    contract: Arc<AggregateCallContract>,
    source: MannSourceParams,
}
impl MannKernel {
    pub(super) fn try_new(
        contract: Arc<AggregateCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, KernelFailure> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(compile_failure)?;
        validate_contract(&contract, &mut work)?;
        let source = MannSourceParams::try_new().map_err(resource_failure)?;
        work.finish().map_err(compile_failure)?;
        Ok(Self { contract, source })
    }
    fn emit<'s, I>(
        &self,
        states: I,
        expected: AggregateInvocationPhase,
        context: &AggregateEmissionContext<'_>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, EvaluationFailure>
    where
        I: ExactSizeIterator<Item = &'s MannState> + Clone,
    {
        let result = (|| {
            if context.phase() != expected {
                return Err(
                    invalid("Mann-Whitney output differs from its exact emission phase").into(),
                );
            }
            let host = context.allocator().ok_or_else(|| {
                invalid("Mann-Whitney emission requires its actual host allocator")
            })?;
            let charge = OpaqueRetainedCharge::try_new(Arc::clone(host))?;
            emit_mann(
                &self.source,
                &self.contract,
                context,
                states.clone(),
                charge,
                control,
            )
        })();
        if result.is_err() {
            for state in states {
                state.latch();
            }
        }
        result
    }
}
fn mapping_copy(
    mapping: &[usize],
    selection: Selection<'_>,
    target: &mut HostVec<usize, HostAggregateAllocator>,
    allocator: &HostAggregateAllocator,
    work: &mut ScalarWork<'_, '_>,
) -> Result<usize, ScalarStateError> {
    if mapping.len() != selection.len() {
        return Err(invalid("Mann-Whitney group mapping differs from its selected domain").into());
    }
    work.flush()?;
    target
        .try_reserve_exact(mapping.len())
        .map_err(|_| allocator.take_failure())?;
    for &index in mapping {
        work.step()?;
        target.push(index);
    }
    let bytes = target
        .capacity()
        .checked_mul(size_of::<usize>())
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok(add(allocator_retained(allocator)?, bytes)?)
}

impl PreparedAggregateKernel for MannKernel {
    type State = MannState;
    type PreparedUpdateBatch<'a> = MannUpdate<'a>;
    type PreparedMergeBatch<'a> = MannMerge<'a>;
    fn contract(&self) -> &Arc<AggregateCallContract> {
        &self.contract
    }
    fn memory_policy(&self) -> AggregateStateMemoryPolicy {
        AggregateStateMemoryPolicy::AllocationTracked
    }
    fn has_invocation_data(&self) -> bool {
        true
    }
    fn requires_empty_update_preparation(&self) -> bool {
        true
    }
    fn requires_emission_context(&self) -> bool {
        true
    }
    fn requires_helper_memory(&self) -> bool {
        true
    }
    fn retained_bytes(&self, state: &Self::State) -> usize {
        state.retained.get()
    }
    fn prepared_update_retained_bytes(&self, prepared: &Self::PreparedUpdateBatch<'_>) -> usize {
        prepared.retained
    }
    fn prepared_merge_retained_bytes(&self, prepared: &Self::PreparedMergeBatch<'_>) -> usize {
        prepared.retained
    }
    fn clone_for_local_phase(
        &self,
        contract: Arc<AggregateCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<Self>, KernelFailure> {
        Ok(Arc::new(Self::try_new(contract, control)?))
    }
    fn create_state(&self, _: &dyn KernelEvaluationControl) -> Result<Self::State, KernelFailure> {
        Err(invalid(
            "Mann-Whitney state requires its actual host allocator",
        ))
    }
    fn create_state_with_allocator(
        &self,
        host: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        control.checkpoint(0)?;
        let host =
            host.ok_or_else(|| invalid("Mann-Whitney state requires its actual host allocator"))?;
        let allocator = HostAggregateAllocator::try_new_with_journal(host)?;
        let base_retained = allocator_retained(&allocator)?;
        Ok(MannState {
            core: RefCell::new(MannWhitneyState::new(allocator)),
            failed: Cell::new(false),
            base_retained,
            retained: Cell::new(base_retained),
        })
    }
    fn prepare_update<'a>(
        &'a self,
        _: SelectedAggregateUpdateInput<'a, 'a>,
        _: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedUpdateBatch<'a>, KernelFailure> {
        Err(invalid(
            "Mann-Whitney requires its lossless selected update port",
        ))
    }
    fn update_row<'a>(
        &self,
        _: &mut Self::State,
        _: &Self::PreparedUpdateBatch<'a>,
        _: usize,
        _: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        Err(invalid(
            "Mann-Whitney requires its lossless selected update port",
        ))
    }
    fn prepare_merge<'a>(
        &'a self,
        _: SelectedAggregateMergeInput<'a, 'a>,
        _: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedMergeBatch<'a>, KernelFailure> {
        Err(invalid(
            "Mann-Whitney requires its lossless selected merge port",
        ))
    }
    fn merge_row<'a>(
        &self,
        _: &mut Self::State,
        _: &Self::PreparedMergeBatch<'a>,
        _: usize,
        _: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        Err(invalid(
            "Mann-Whitney requires its lossless selected merge port",
        ))
    }
    fn build_intermediate<'s, I>(
        &self,
        _: I,
        _: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        Self::State: 's,
        I: ExactSizeIterator<Item = &'s Self::State>,
    {
        Err(invalid("Mann-Whitney requires its actual emission context"))
    }
    fn build_final<'s, I>(
        &self,
        _: I,
        _: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        Self::State: 's,
        I: ExactSizeIterator<Item = &'s Self::State>,
    {
        Err(invalid("Mann-Whitney requires its actual emission context"))
    }
    fn prepare_update_evaluation<'a>(
        &'a self,
        input: SelectedAggregateUpdateInput<'a, 'a>,
        mapping: &[usize],
        host: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedUpdateBatch<'a>, EvaluationFailure> {
        let host =
            host.ok_or_else(|| invalid("Mann-Whitney update requires its actual host allocator"))?;
        let allocator = HostAggregateAllocator::try_new_with_journal(Arc::clone(&host))?;
        let charge = OpaqueRetainedCharge::try_new(Arc::clone(&host))?;
        let mut owned_mapping = HostVec::new_in(allocator.clone());
        let mut retained = 0;
        let view = call_input(
            &self.source,
            &charge,
            MannDataDomain {
                allocator: &allocator,
                contract: &self.contract,
                phase: AggregateInvocationPhase::Update,
                selection: input.selection(),
                mapping,
            },
            control,
            |work, resources| {
                if !std::ptr::eq(input.contract(), self.contract.as_ref())
                    || !self.contract.phase().consumes_logical_arguments()
                    || !input.order_arguments().is_empty()
                    || input.logical_arguments().len()
                        != self.contract.call().logical_argument_count()
                {
                    return Err(invalid(
                        "Mann-Whitney update differs from its exact phase or channels",
                    )
                    .into());
                }
                retained = mapping_copy(
                    mapping,
                    input.selection(),
                    &mut owned_mapping,
                    &allocator,
                    work,
                )?;
                let channels = std::array::from_fn(|i| {
                    input.logical_arguments().get(i).map(|arg| arg.array())
                });
                MannWhitneyInput::try_new_channels_with_resources(
                    channels,
                    input.logical_arguments().len(),
                    work,
                    resources,
                )
            },
        )?;
        Ok(MannUpdate {
            input,
            view,
            mapping: owned_mapping,
            allocator,
            host,
            charge,
            retained,
        })
    }
    fn update_row_evaluation<'a>(
        &self,
        state: &mut Self::State,
        prepared: &Self::PreparedUpdateBatch<'a>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), EvaluationFailure> {
        if state.failed.get() {
            return Err(KernelFailure::InstanceFailed.into());
        }
        let result = call_unit(
            &self.source,
            &prepared.charge,
            MannDataDomain {
                allocator: &prepared.allocator,
                contract: &self.contract,
                phase: AggregateInvocationPhase::Update,
                selection: prepared.input.selection(),
                mapping: &prepared.mapping,
            },
            control,
            |work, resources| {
                let mut core = state
                    .core
                    .try_borrow_mut()
                    .map_err(|_| internal("Mann-Whitney state has a conflicting update borrow"))?;
                if !core.allocator.has_host_authority(&prepared.host) {
                    return Err(
                        invalid("Mann-Whitney update uses another state host authority").into(),
                    );
                }
                let row = prepared.input.selection().row(ordinal).ok_or_else(|| {
                    invalid("Mann-Whitney selected update ordinal is out of bounds")
                })?;
                let mut addresses = [0; 4];
                for (i, arg) in prepared.input.logical_arguments().iter().enumerate() {
                    work.step()?;
                    addresses[i] = arg.value_row(ordinal, row);
                    if addresses[i] >= arg.array().len() {
                        return Err(internal(
                            "Mann-Whitney selected update address is out of bounds",
                        )
                        .into());
                    }
                }
                prepared
                    .view
                    .update_row_with_resources(&mut core, addresses, work, resources)?;
                state.account(&core)?;
                Ok(())
            },
        );
        if result.is_err() {
            state.latch();
        }
        result
    }
    fn prepare_merge_evaluation<'a>(
        &'a self,
        input: SelectedAggregateMergeInput<'a, 'a>,
        mapping: &[usize],
        host: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedMergeBatch<'a>, EvaluationFailure> {
        let host =
            host.ok_or_else(|| invalid("Mann-Whitney merge requires its actual host allocator"))?;
        let allocator = HostAggregateAllocator::try_new_with_journal(Arc::clone(&host))?;
        let charge = OpaqueRetainedCharge::try_new(Arc::clone(&host))?;
        let mut owned_mapping = HostVec::new_in(allocator.clone());
        let mut retained = 0;
        call_unit(
            &self.source,
            &charge,
            MannDataDomain {
                allocator: &allocator,
                contract: &self.contract,
                phase: AggregateInvocationPhase::Merge,
                selection: input.selection(),
                mapping,
            },
            control,
            |work, _| {
                if !std::ptr::eq(input.contract(), self.contract.as_ref())
                    || self.contract.phase().consumes_logical_arguments()
                {
                    return Err(invalid("Mann-Whitney merge differs from its exact phase").into());
                }
                if input
                    .state()
                    .array()
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .is_none()
                {
                    return Err(
                        invalid("Mann-Whitney merge requires its exact Binary carrier").into(),
                    );
                }
                retained = mapping_copy(
                    mapping,
                    input.selection(),
                    &mut owned_mapping,
                    &allocator,
                    work,
                )?;
                Ok(())
            },
        )?;
        Ok(MannMerge {
            input,
            mapping: owned_mapping,
            allocator,
            host,
            charge,
            retained,
        })
    }
    fn merge_row_evaluation<'a>(
        &self,
        state: &mut Self::State,
        prepared: &Self::PreparedMergeBatch<'a>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), EvaluationFailure> {
        if state.failed.get() {
            return Err(KernelFailure::InstanceFailed.into());
        }
        let result = call_unit(
            &self.source,
            &prepared.charge,
            MannDataDomain {
                allocator: &prepared.allocator,
                contract: &self.contract,
                phase: AggregateInvocationPhase::Merge,
                selection: prepared.input.selection(),
                mapping: &prepared.mapping,
            },
            control,
            |work, resources| {
                let mut core = state
                    .core
                    .try_borrow_mut()
                    .map_err(|_| internal("Mann-Whitney state has a conflicting merge borrow"))?;
                if !core.allocator.has_host_authority(&prepared.host) {
                    return Err(
                        invalid("Mann-Whitney merge uses another state host authority").into(),
                    );
                }
                let row = prepared.input.selection().row(ordinal).ok_or_else(|| {
                    invalid("Mann-Whitney selected merge ordinal is out of bounds")
                })?;
                let argument = prepared.input.state();
                let address = argument.value_row(ordinal, row);
                let array = argument
                    .array()
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| {
                        invalid("Mann-Whitney merge requires its exact Binary carrier")
                    })?;
                if address >= array.len() {
                    return Err(
                        internal("Mann-Whitney selected merge address is out of bounds").into(),
                    );
                }
                work.step()?;
                if array.is_null(address) {
                    return Ok(());
                }
                // Preserve the original target allocator, including uninitialized
                // replacement. Prepared batch metadata never becomes state metadata.
                let decoded = MannWhitneyState::deserialize_in_with_resources(
                    array.value(address),
                    core.allocator.clone(),
                    work,
                    resources,
                )?;
                core.merge_with_resources(decoded, work, resources)?;
                state.account(&core)?;
                Ok(())
            },
        );
        if result.is_err() {
            state.latch();
        }
        result
    }
    fn build_intermediate_evaluation_with_context<'s, I>(
        &self,
        states: I,
        context: &AggregateEmissionContext<'_>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, EvaluationFailure>
    where
        Self::State: 's,
        I: ExactSizeIterator<Item = &'s Self::State> + Clone,
    {
        self.emit(
            states,
            AggregateInvocationPhase::Intermediate,
            context,
            control,
        )
    }
    fn build_final_evaluation_with_context<'s, I>(
        &self,
        states: I,
        context: &AggregateEmissionContext<'_>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, EvaluationFailure>
    where
        Self::State: 's,
        I: ExactSizeIterator<Item = &'s Self::State> + Clone,
    {
        self.emit(states, AggregateInvocationPhase::Final, context, control)
    }
}

pub(super) fn validate_contract(
    contract: &AggregateCallContract,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    work.step().map_err(compile_failure)?;
    let call = contract.call();
    let selected = call.selected();
    if call.function_id().as_str() != "builtin.aggregate/mann_whitney_u_test/v1"
        || selected.overload.as_str() != "builtin.aggregate/mann_whitney_u_test/derived-v1"
        || !(2..=4).contains(&call.logical_argument_count())
        || selected.argument_types.len() != call.logical_argument_count()
        || contract.distinct()
        || !contract.order_keys().is_empty()
        || selected
            .argument_types
            .iter()
            .any(|t| !matches!(t, FunctionArgumentType::Value(_)))
        || contract.state_format().as_str() != "novarocks/mann_whitney_u_test/state-v1"
    {
        return Err(invalid(
            "Mann-Whitney requires its original full aggregate declaration",
        ));
    }
    let effects = call.effects();
    if effects.value_stability != FunctionVolatility::Immutable
        || effects.own_row_error != FunctionIntrinsicRowError::NotRowEvaluated
        || effects.failure_behavior != FunctionFailureBehavior::Propagate
        || effects.null_behavior != novarocks_type_contract::FunctionNullBehavior::CalledOnNull
        || effects.argument_control != novarocks_type_contract::ArgumentControl::Aggregate
        || effects.instance_state
            != novarocks_type_contract::FunctionInstanceState::AggregateInstance
        || effects.observable_effects != novarocks_type_contract::ObservableEffects::NONE
        || !effects.environment.is_empty()
    {
        return Err(invalid(
            "Mann-Whitney call effects differ from its original declaration",
        ));
    }
    work.step().map_err(compile_failure)?;
    let FunctionResultType::Scalar(output) = &selected.result_type else {
        return Err(invalid("Mann-Whitney requires its exact scalar result"));
    };
    if !FunctionValueType::new(DataType::Utf8, true)
        .exactly_equals_observed::<KernelFailure>(output, || work.step().map_err(compile_failure))?
        || !FunctionValueType::new(DataType::Binary, true)
            .exactly_equals_observed::<KernelFailure>(contract.intermediate_type(), || {
                work.step().map_err(compile_failure)
            })?
    {
        return Err(invalid(
            "Mann-Whitney result or state differs from its full type",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
