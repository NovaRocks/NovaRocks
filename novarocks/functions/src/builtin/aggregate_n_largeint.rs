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
//! Exact, initially unregistered LARGEINT profile over the sole N core.
use super::aggregate_n_core::NState as CoreNState;
use super::aggregate_n_largeint_emission::{NEmissionAccess, emit_n};
use super::aggregate_n_largeint_operation::{NDataDomain, call_unit};
use super::aggregate_n_largeint_resources::{
    BoundNCodecResources, BoundNSortResources, add, operation_prefix_bytes,
};
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_scalar::{ScalarStateError, ScalarWork, TrackedAggScalarValue};
use crate::kernel_control::{compile_failure, internal, invalid};
use crate::opaque_memory::OpaqueRetainedCharge;
use crate::*;
use allocator_api2::vec::Vec as HostVec;
use arrow_array::{Array, ArrayRef, BinaryArray};
use arrow_schema::DataType;
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, PureCompileControl, ValueLogicalType,
};
use std::{
    cell::{Cell, RefCell},
    sync::Arc,
};

pub(super) struct NLargeIntState {
    contract: Arc<AggregateCallContract>,
    core: RefCell<CoreNState<HostAggregateAllocator>>,
    failed: Cell<bool>,
    base_retained: usize,
    retained: Cell<usize>,
}
fn allocator_retained(allocator: &HostAggregateAllocator) -> Result<usize, KernelFailure> {
    add(
        allocator.metadata_bytes(),
        allocator.journal_backing_bytes(),
    )
}
impl NLargeIntState {
    pub(super) fn retained_bytes(&self) -> usize {
        self.retained.get()
    }
    fn account(
        &self,
        core: &CoreNState<HostAggregateAllocator>,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), ScalarStateError> {
        let vector = core
            .values
            .capacity()
            .checked_mul(size_of::<TrackedAggScalarValue<HostAggregateAllocator>>())
            .ok_or(KernelFailure::ResourceExhausted)?;
        let mut bytes = add(self.base_retained, vector)?;
        for value in &core.values {
            work.step()?;
            if let TrackedAggScalarValue::Utf8(value) = value {
                bytes = add(bytes, value.capacity())?;
            }
        }
        self.retained.set(bytes);
        Ok(())
    }
    fn latch(&self) {
        self.failed.set(true);
        if let Ok(mut core) = self.core.try_borrow_mut() {
            core.latch_failure();
            self.retained.set(self.base_retained);
        }
        // A conflicting borrow remains real ownership until it exits; no
        // invisible backing is falsely released or reclassified here.
    }
}
impl NEmissionAccess for NLargeIntState {
    fn latch(&self) {
        NLargeIntState::latch(self);
    }
    fn validate(
        &self,
        contract: &Arc<AggregateCallContract>,
        host: &Arc<dyn AggregateStateAllocator>,
    ) -> Result<(), KernelFailure> {
        if self.failed.get() {
            return Err(KernelFailure::InstanceFailed);
        }
        if !Arc::ptr_eq(contract, &self.contract) {
            return Err(invalid(
                "min_n/max_n state uses another exact profile or phase",
            ));
        }
        let core = self
            .core
            .try_borrow()
            .map_err(|_| internal("min_n/max_n state has a conflicting emission borrow"))?;
        if !core.allocator.has_host_authority(host) {
            return Err(invalid("min_n/max_n state uses another host authority"));
        }
        Ok(())
    }
    fn with_core(
        &self,
        operation: impl FnOnce(&CoreNState<HostAggregateAllocator>) -> Result<(), ScalarStateError>,
    ) -> Result<(), ScalarStateError> {
        if self.failed.get() {
            return Err(KernelFailure::InstanceFailed.into());
        }
        let core = self
            .core
            .try_borrow()
            .map_err(|_| internal("min_n/max_n state has a conflicting emission borrow"))?;
        operation(&core)
    }
}
pub(super) struct NLargeIntUpdate<'a> {
    input: SelectedAggregateUpdateInput<'a, 'a>,
    mapping: HostVec<usize, HostAggregateAllocator>,
    allocator: HostAggregateAllocator,
    host: Arc<dyn AggregateStateAllocator>,
    charge: OpaqueRetainedCharge,
    retained: usize,
}
pub(super) struct NLargeIntMerge<'a> {
    input: SelectedAggregateMergeInput<'a, 'a>,
    mapping: HostVec<usize, HostAggregateAllocator>,
    allocator: HostAggregateAllocator,
    host: Arc<dyn AggregateStateAllocator>,
    charge: OpaqueRetainedCharge,
    retained: usize,
}
impl NLargeIntUpdate<'_> {
    pub(super) fn retained_bytes(&self) -> usize {
        self.retained
    }
}
impl NLargeIntMerge<'_> {
    pub(super) fn retained_bytes(&self) -> usize {
        self.retained
    }
}
#[derive(Debug)]
pub(super) struct NLargeIntKernel {
    pub(super) contract: Arc<AggregateCallContract>,
    pub(super) keep_smallest: bool,
}
impl NLargeIntKernel {
    pub(super) fn try_new(
        contract: Arc<AggregateCallContract>,
        keep_smallest: bool,
        control: &dyn PureCompileControl,
    ) -> Result<Self, KernelFailure> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(compile_failure)?;
        validate_contract(&contract, keep_smallest, &mut work)?;
        work.finish().map_err(compile_failure)?;
        Ok(Self {
            contract,
            keep_smallest,
        })
    }
    pub(super) fn emit<'s, S, I>(
        &self,
        states: I,
        expected: AggregateInvocationPhase,
        context: &AggregateEmissionContext<'_>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, EvaluationFailure>
    where
        S: NEmissionAccess + 's,
        I: ExactSizeIterator<Item = &'s S> + Clone,
    {
        let result = (|| {
            if context.phase() != expected {
                return Err(
                    invalid("min_n/max_n output differs from its exact emission phase").into(),
                );
            }
            let host = context.allocator().ok_or_else(|| {
                invalid("min_n/max_n emission requires its actual host allocator")
            })?;
            emit_n(
                &self.contract,
                context,
                states.clone(),
                OpaqueRetainedCharge::try_new(Arc::clone(host))?,
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
        return Err(invalid("min_n/max_n mapping differs from its selected domain").into());
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
impl PreparedAggregateKernel for NLargeIntKernel {
    type State = NLargeIntState;
    type PreparedUpdateBatch<'a> = NLargeIntUpdate<'a>;
    type PreparedMergeBatch<'a> = NLargeIntMerge<'a>;
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
    fn prepared_update_retained_bytes(&self, batch: &Self::PreparedUpdateBatch<'_>) -> usize {
        batch.retained
    }
    fn prepared_merge_retained_bytes(&self, batch: &Self::PreparedMergeBatch<'_>) -> usize {
        batch.retained
    }
    fn clone_for_local_phase(
        &self,
        contract: Arc<AggregateCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<Self>, KernelFailure> {
        if !Arc::ptr_eq(self.contract.call(), contract.call()) {
            return Err(invalid(
                "min_n/max_n local phase replaced its exact source call",
            ));
        }
        Ok(Arc::new(Self::try_new(
            contract,
            self.keep_smallest,
            control,
        )?))
    }
    fn create_state(&self, _: &dyn KernelEvaluationControl) -> Result<Self::State, KernelFailure> {
        Err(invalid(
            "min_n/max_n LARGEINT state requires its actual host allocator",
        ))
    }
    fn create_state_with_allocator(
        &self,
        host: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        let host = host.ok_or_else(|| {
            invalid("min_n/max_n LARGEINT state requires its actual host allocator")
        })?;
        let charge = OpaqueRetainedCharge::try_new(Arc::clone(&host))?;
        let _prefix = charge.reserve_operation(operation_prefix_bytes()?)?;
        control.checkpoint(0)?;
        let allocator = HostAggregateAllocator::try_new_with_journal(host)?;
        let base_retained = allocator_retained(&allocator)?;
        control.checkpoint(0)?;
        Ok(NLargeIntState {
            contract: Arc::clone(&self.contract),
            core: RefCell::new(CoreNState::new(allocator)),
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
            "min_n/max_n LARGEINT requires its lossless selected update port",
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
            "min_n/max_n LARGEINT requires its lossless selected update port",
        ))
    }
    fn prepare_merge<'a>(
        &'a self,
        _: SelectedAggregateMergeInput<'a, 'a>,
        _: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedMergeBatch<'a>, KernelFailure> {
        Err(invalid(
            "min_n/max_n LARGEINT requires its lossless selected merge port",
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
            "min_n/max_n LARGEINT requires its lossless selected merge port",
        ))
    }
    fn build_intermediate<'s, I>(
        &self,
        _: I,
        _: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'s Self::State>,
    {
        Err(invalid(
            "min_n/max_n LARGEINT requires its actual emission context",
        ))
    }
    fn build_final<'s, I>(
        &self,
        _: I,
        _: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'s Self::State>,
    {
        Err(invalid(
            "min_n/max_n LARGEINT requires its actual emission context",
        ))
    }

    fn prepare_update_evaluation<'a>(
        &'a self,
        input: SelectedAggregateUpdateInput<'a, 'a>,
        mapping: &[usize],
        host: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedUpdateBatch<'a>, EvaluationFailure> {
        let host =
            host.ok_or_else(|| invalid("min_n/max_n update requires its actual host allocator"))?;
        let allocator = HostAggregateAllocator::try_new_with_journal(Arc::clone(&host))?;
        let charge = OpaqueRetainedCharge::try_new(Arc::clone(&host))?;
        let mut owned = HostVec::new_in(allocator.clone());
        let mut retained = 0;
        call_unit(
            &charge,
            &allocator,
            NDataDomain::Mutation {
                contract: &self.contract,
                phase: AggregateInvocationPhase::Update,
                selection: input.selection(),
                mapping,
            },
            control,
            |work| {
                if !std::ptr::eq(input.contract(), self.contract.as_ref())
                    || !self.contract.phase().consumes_logical_arguments()
                    || input.logical_arguments().len() != 2
                    || !input.order_arguments().is_empty()
                {
                    return Err(invalid(
                        "min_n/max_n update differs from its exact phase or channels",
                    )
                    .into());
                }
                retained = mapping_copy(mapping, input.selection(), &mut owned, &allocator, work)?;
                Ok(())
            },
        )?;
        Ok(NLargeIntUpdate {
            input,
            mapping: owned,
            allocator,
            host,
            charge,
            retained,
        })
    }
    fn update_row_evaluation<'a>(
        &self,
        state: &mut Self::State,
        batch: &Self::PreparedUpdateBatch<'a>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), EvaluationFailure> {
        if state.failed.get() {
            return Err(KernelFailure::InstanceFailed.into());
        }
        let result = call_unit(
            &batch.charge,
            &batch.allocator,
            NDataDomain::Mutation {
                contract: &self.contract,
                phase: AggregateInvocationPhase::Update,
                selection: batch.input.selection(),
                mapping: &batch.mapping,
            },
            control,
            |work| {
                state.validate(&self.contract, &batch.host)?;
                if !std::ptr::eq(batch.input.contract(), self.contract.as_ref()) {
                    return Err(invalid("min_n/max_n update frame uses another exact call").into());
                }
                let mut core = state
                    .core
                    .try_borrow_mut()
                    .map_err(|_| internal("min_n/max_n state has a conflicting update borrow"))?;
                let row = batch
                    .input
                    .selection()
                    .row(ordinal)
                    .ok_or_else(|| invalid("min_n/max_n selected ordinal is out of bounds"))?;
                let values = batch.input.logical_arguments()[0];
                let limits = batch.input.logical_arguments()[1];
                let value_row = values.value_row(ordinal, row);
                let limit_row = limits.value_row(ordinal, row);
                if value_row >= values.array().len() || limit_row >= limits.array().len() {
                    return Err(
                        internal("min_n/max_n selected channel address is out of bounds").into(),
                    );
                }
                work.step()?;
                let mut sort = BoundNSortResources {
                    charge: &batch.charge,
                    allocator: &core.allocator.clone(),
                };
                core.update_from_arrays_with_sort_resources(
                    values.array(),
                    value_row,
                    limits.array(),
                    limit_row,
                    self.keep_smallest,
                    &mut sort,
                    work,
                )?;
                state.account(&core, work)
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
            host.ok_or_else(|| invalid("min_n/max_n merge requires its actual host allocator"))?;
        let allocator = HostAggregateAllocator::try_new_with_journal(Arc::clone(&host))?;
        let charge = OpaqueRetainedCharge::try_new(Arc::clone(&host))?;
        let mut owned = HostVec::new_in(allocator.clone());
        let mut retained = 0;
        call_unit(
            &charge,
            &allocator,
            NDataDomain::Mutation {
                contract: &self.contract,
                phase: AggregateInvocationPhase::Merge,
                selection: input.selection(),
                mapping,
            },
            control,
            |work| {
                if !std::ptr::eq(input.contract(), self.contract.as_ref())
                    || self.contract.phase().consumes_logical_arguments()
                    || input.state().array().data_type() != &DataType::Binary
                {
                    return Err(invalid(
                        "min_n/max_n merge differs from its exact phase or Binary state",
                    )
                    .into());
                }
                retained = mapping_copy(mapping, input.selection(), &mut owned, &allocator, work)?;
                Ok(())
            },
        )?;
        Ok(NLargeIntMerge {
            input,
            mapping: owned,
            allocator,
            host,
            charge,
            retained,
        })
    }
    fn merge_row_evaluation<'a>(
        &self,
        state: &mut Self::State,
        batch: &Self::PreparedMergeBatch<'a>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), EvaluationFailure> {
        if state.failed.get() {
            return Err(KernelFailure::InstanceFailed.into());
        }
        let result = call_unit(
            &batch.charge,
            &batch.allocator,
            NDataDomain::Mutation {
                contract: &self.contract,
                phase: AggregateInvocationPhase::Merge,
                selection: batch.input.selection(),
                mapping: &batch.mapping,
            },
            control,
            |work| {
                state.validate(&self.contract, &batch.host)?;
                if !std::ptr::eq(batch.input.contract(), self.contract.as_ref()) {
                    return Err(invalid("min_n/max_n merge frame uses another exact call").into());
                }
                let mut core = state
                    .core
                    .try_borrow_mut()
                    .map_err(|_| internal("min_n/max_n state has a conflicting merge borrow"))?;
                let row = batch.input.selection().row(ordinal).ok_or_else(|| {
                    invalid("min_n/max_n selected merge ordinal is out of bounds")
                })?;
                let argument = batch.input.state();
                let address = argument.value_row(ordinal, row);
                let array = argument
                    .array()
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| {
                        invalid("min_n/max_n merge requires its exact Binary carrier")
                    })?;
                if address >= array.len() {
                    return Err(
                        internal("min_n/max_n selected merge address is out of bounds").into(),
                    );
                }
                work.step()?;
                if array.is_null(address) {
                    return Ok(());
                }
                let allocator = core.allocator.clone();
                let mut sort = BoundNSortResources {
                    charge: &batch.charge,
                    allocator: &allocator,
                };
                let mut codec = BoundNCodecResources {
                    charge: &batch.charge,
                    allocator: &allocator,
                };
                core.merge_from_array_with_resources(
                    argument.array(),
                    address,
                    self.keep_smallest,
                    &mut sort,
                    &mut codec,
                    work,
                )?;
                state.account(&core, work)
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
        I: ExactSizeIterator<Item = &'s Self::State> + Clone,
    {
        self.emit(states, AggregateInvocationPhase::Final, context, control)
    }
}
pub(super) fn validate_contract(
    contract: &AggregateCallContract,
    keep_smallest: bool,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    let [
        FunctionArgumentType::Value(value),
        FunctionArgumentType::Value(limit),
    ] = contract.call().selected().argument_types.as_ref()
    else {
        return Err(invalid(
            "min_n/max_n LARGEINT requires exactly two value arguments",
        ));
    };
    work.step().map_err(compile_failure)?;
    let name = if keep_smallest { "min_n" } else { "max_n" };
    if contract.call().function_id().as_str() != format!("builtin.aggregate/{name}/v1")
        || contract.call().selected().overload.as_str()
            != format!("builtin.aggregate/{name}/derived-v1")
        || contract.call().kind() != FunctionKind::Aggregate
        || contract.call().logical_argument_count() != 2
        || contract.distinct()
        || !contract.order_keys().is_empty()
        || contract
            .state_interpretation()
            .is_some_and(|state| state.distinct || !state.order_keys.is_empty())
        || value.logical_type != ValueLogicalType::LargeInt
        || value.data_type != DataType::FixedSizeBinary(16)
        || limit.logical_type != ValueLogicalType::Physical
        || !matches!(
            limit.data_type,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        )
    {
        return Err(invalid(
            "min_n/max_n has no installed LARGEINT profile for this exact call",
        ));
    }
    let effects = contract.call().effects();
    let expected_effects = super::aggregate_n_owner::effects();
    if effects.value_stability != expected_effects.value_stability
        || effects.own_row_error != expected_effects.own_row_error
        || effects.failure_behavior != expected_effects.failure_behavior
        || effects.null_behavior != expected_effects.null_behavior
        || effects.argument_control != expected_effects.argument_control
        || effects.instance_state != expected_effects.instance_state
        || effects.observable_effects != expected_effects.observable_effects
        || !effects.environment.is_empty()
    {
        return Err(invalid(
            "min_n/max_n call effects differ from its exact declaration",
        ));
    }
    work.step().map_err(compile_failure)?;
    let expected = FunctionValueType::new(
        DataType::List(Arc::new(super::signature::value_field("item", value, true))),
        true,
    );
    if !expected.exactly_equals_observed::<KernelFailure>(contract.final_type(), || {
        work.step().map_err(compile_failure)
    })? || !FunctionValueType::new(DataType::Binary, true)
        .exactly_equals_observed::<KernelFailure>(contract.intermediate_type(), || {
            work.step().map_err(compile_failure)
        })?
        || contract.state_format().as_str() != format!("novarocks/{name}/state-v1")
    {
        return Err(invalid(
            "min_n/max_n LARGEINT result or state differs from its exact declaration",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
