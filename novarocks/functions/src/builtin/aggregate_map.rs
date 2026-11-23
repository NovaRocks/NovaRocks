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

//! Full generic MAP_AGG consumer of the original shared computation.
use super::aggregate_map_core as core;
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_invocation_backing::HostDiagnostic;
use crate::aggregate_scalar::{self as scalar, ScalarWork};
use crate::kernel_control::{compile_failure, internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::*;
use allocator_api2::vec::Vec as HostVec;
use arrow_array::ArrayRef;
use arrow_schema::DataType;
use novarocks_type_contract::CompileCheckpoints;
use std::sync::Arc;

#[derive(Debug)]
pub(super) struct MapKernel {
    pub(super) contract: Arc<AggregateCallContract>,
}
pub(super) struct MapState {
    core: core::MapAggState<HostAggregateAllocator>,
    key_bytes: usize,
    heap_bytes: usize,
    failed: bool,
}
impl MapState {
    fn latch(&mut self) {
        self.failed = true;
        self.core = core::MapAggState::new(self.core.allocator.clone());
        self.key_bytes = 0;
        self.heap_bytes = 0;
    }
    fn account_new_values(
        &mut self,
        from: usize,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), KernelFailure> {
        for (key, value) in &self.core.entries[from..] {
            for scalar in std::iter::once(key).chain(value.iter()) {
                self.heap_bytes = self
                    .heap_bytes
                    .checked_add(scalar::tracked_scalar_heap_capacity(scalar, work)?)
                    .ok_or(KernelFailure::ResourceExhausted)?;
            }
        }
        Ok(())
    }
}
pub(super) struct MapUpdate<'a> {
    input: SelectedAggregateUpdateInput<'a, 'a>,
    mapping: HostVec<usize, HostAggregateAllocator>,
    allocator: HostAggregateAllocator,
}
pub(super) struct MapMerge<'a> {
    input: SelectedAggregateMergeInput<'a, 'a>,
    mapping: HostVec<usize, HostAggregateAllocator>,
    allocator: HostAggregateAllocator,
}
enum FailureDomain<'a, 'host> {
    Mutation {
        contract: &'a Arc<AggregateCallContract>,
        phase: AggregateInvocationPhase,
        selection: Selection<'a>,
        mapping: &'a [usize],
    },
    Emission(&'a AggregateEmissionContext<'host>),
}
struct FailureSink<'a, 'host> {
    allocator: &'a HostAggregateAllocator,
    domain: FailureDomain<'a, 'host>,
    control: &'a dyn KernelEvaluationControl,
}
impl FailureSink<'_, '_> {
    fn diagnostic<T: std::fmt::Display + ?Sized>(&mut self, original: &T) -> EvaluationFailure {
        use crate::aggregate_format::AggregateFailureStage;
        let stage = match &self.domain {
            FailureDomain::Mutation {
                phase: AggregateInvocationPhase::Update,
                ..
            } => AggregateFailureStage::Update,
            FailureDomain::Mutation {
                phase: AggregateInvocationPhase::Merge,
                ..
            } => AggregateFailureStage::Merge,
            FailureDomain::Emission(context) => match context.phase() {
                AggregateInvocationPhase::Intermediate => AggregateFailureStage::BuildIntermediate,
                AggregateInvocationPhase::Final => AggregateFailureStage::BuildFinal,
                _ => return invalid("map_agg emission diagnostic has a foreign phase").into(),
            },
            _ => return invalid("map_agg mutation diagnostic has a foreign phase").into(),
        };
        // Recipe detection is not a published Data capsule. The following is
        // real host-backed original batch diagnostic construction work.
        let mut work = EvaluationCheckpoints::new(self.control);
        let result = (|| {
            let text = HostDiagnostic::prepare(self.allocator, &mut work, |writer| {
                write!(writer, "{}", stage.message(original))
            })?;
            match &self.domain {
                FailureDomain::Mutation {
                    contract,
                    phase,
                    selection,
                    mapping,
                } => InvocationData::prepare_aggregate(
                    self.allocator,
                    Arc::clone(contract),
                    *phase,
                    *selection,
                    mapping,
                    text,
                    &mut work,
                ),
                FailureDomain::Emission(context) => {
                    InvocationData::prepare_emission(self.allocator, context, text, &mut work)
                }
            }
        })();
        match result {
            Ok(data) => EvaluationFailure::InvocationData(data),
            Err(cause) => cause.into(),
        }
    }
}
impl FailureSink<'_, '_> {
    fn scalar(&mut self, origin: KernelFailure) -> EvaluationFailure {
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        if origin.failure_fact().class() != FailureClass::Data {
            return origin.into();
        }
        let phase = match &self.domain {
            FailureDomain::Mutation { phase, .. } => *phase,
            FailureDomain::Emission(context) => context.phase(),
        };
        match self.diagnostic(&origin) {
            EvaluationFailure::InvocationData(data) => data.with_whole_value_source(origin).into(),
            EvaluationFailure::Kernel(diagnostic) => KernelFailure::from_domain(
                super::aggregate_map_source_failure::MapDiagnosticFailure {
                    origin,
                    _diagnostic: diagnostic,
                    phase,
                },
            )
            .into(),
        }
    }
}
fn observed<T>(
    control: &dyn KernelEvaluationControl,
    operation: impl FnOnce(&mut EvaluationCheckpoints<'_>) -> Result<T, EvaluationFailure>,
) -> Result<T, EvaluationFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = operation(&mut work);
    // Whole Data and originating Kernel failures have no optional footer.
    if result.is_ok() {
        work.finish()?;
    }
    result
}
fn mapped_domain(
    mapping: &[usize],
    selection: Selection<'_>,
    host: Option<Arc<dyn AggregateStateAllocator>>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<
    (
        HostAggregateAllocator,
        HostVec<usize, HostAggregateAllocator>,
    ),
    KernelFailure,
> {
    if mapping.len() != selection.len() {
        return Err(invalid("map_agg mapping differs from actual selection"));
    }
    let allocator = HostAggregateAllocator::try_new(
        host.ok_or_else(|| invalid("map_agg requires its host allocator"))?,
    )?;
    let mut owned = HostVec::new_in(allocator.clone());
    if !mapping.is_empty() {
        work.flush()?;
        owned
            .try_reserve_exact(mapping.len())
            .map_err(|_| allocator.take_failure())?;
        work.flush()?;
    }
    for index in mapping {
        owned.push(*index);
        work.step()?;
    }
    Ok((allocator, owned))
}
impl MapKernel {
    fn check_emit(
        &self,
        context: &AggregateEmissionContext<'_>,
        count: usize,
    ) -> Result<(), KernelFailure> {
        if !Arc::ptr_eq(context.contract(), &self.contract)
            || context.state_indices().len() != count
        {
            return Err(invalid(
                "map_agg emission differs from its real prepared contract or state order",
            ));
        }
        Ok(())
    }
}
impl PreparedAggregateKernel for MapKernel {
    fn clone_for_local_phase(
        &self,
        contract: Arc<AggregateCallContract>,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Arc<Self>, KernelFailure> {
        let mut work = CompileCheckpoints::try_new(
            control,
            novarocks_type_contract::CompilePhase::FunctionSpecialization,
        )
        .map_err(compile_failure)?;
        if !Arc::ptr_eq(contract.call(), self.contract.call()) {
            return Err(invalid(
                "map_agg local phase differs from its original call identity",
            ));
        }
        work.step().map_err(compile_failure)?;
        validate_contract(&contract, &mut work)?;
        work.finish().map_err(compile_failure)?;
        Ok(Arc::new(Self { contract }))
    }
    type State = MapState;
    type PreparedUpdateBatch<'a> = MapUpdate<'a>;
    type PreparedMergeBatch<'a> = MapMerge<'a>;
    fn contract(&self) -> &Arc<AggregateCallContract> {
        &self.contract
    }
    fn memory_policy(&self) -> AggregateStateMemoryPolicy {
        AggregateStateMemoryPolicy::AllocationTracked
    }
    fn retained_bytes(&self, state: &Self::State) -> usize {
        state.core.allocator.metadata_bytes()
            + state.core.seen_keys.raw_table().allocation_info().1.size()
            + state.core.entries.capacity()
                * std::mem::size_of::<(
                    scalar::TrackedAggScalarValue<HostAggregateAllocator>,
                    Option<scalar::TrackedAggScalarValue<HostAggregateAllocator>>,
                )>()
            + state.key_bytes
            + state.heap_bytes
    }
    fn prepared_update_retained_bytes(&self, prepared: &Self::PreparedUpdateBatch<'_>) -> usize {
        prepared.allocator.metadata_bytes()
            + prepared.mapping.capacity() * std::mem::size_of::<usize>()
    }
    fn prepared_merge_retained_bytes(&self, prepared: &Self::PreparedMergeBatch<'_>) -> usize {
        prepared.allocator.metadata_bytes()
            + prepared.mapping.capacity() * std::mem::size_of::<usize>()
    }
    fn has_invocation_data(&self) -> bool {
        true
    }
    fn requires_emission_context(&self) -> bool {
        true
    }
    fn create_state(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        control.checkpoint(0)?;
        Err(invalid("map_agg requires its host allocator"))
    }
    fn create_state_with_allocator(
        &self,
        allocator: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        control.checkpoint(0)?;
        let host = HostAggregateAllocator::try_new(
            allocator.ok_or_else(|| invalid("map_agg requires its host allocator"))?,
        )?;
        Ok(MapState {
            core: core::MapAggState::new(host),
            key_bytes: 0,
            heap_bytes: 0,
            failed: false,
        })
    }
    fn prepare_update<'a>(
        &'a self,
        _input: SelectedAggregateUpdateInput<'a, 'a>,
        _control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedUpdateBatch<'a>, KernelFailure> {
        Err(invalid(
            "map_agg requires the lossless aggregate update port",
        ))
    }
    fn update_row<'a>(
        &self,
        _state: &mut Self::State,
        _input: &Self::PreparedUpdateBatch<'a>,
        _ordinal: usize,
        _control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        Err(invalid(
            "map_agg requires the lossless aggregate update port",
        ))
    }
    fn prepare_merge<'a>(
        &'a self,
        _input: SelectedAggregateMergeInput<'a, 'a>,
        _control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedMergeBatch<'a>, KernelFailure> {
        Err(invalid(
            "map_agg requires the lossless aggregate merge port",
        ))
    }
    fn merge_row<'a>(
        &self,
        _state: &mut Self::State,
        _input: &Self::PreparedMergeBatch<'a>,
        _ordinal: usize,
        _control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        Err(invalid(
            "map_agg requires the lossless aggregate merge port",
        ))
    }
    fn build_intermediate<'s, I>(
        &self,
        _states: I,
        _control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        Self::State: 's,
        I: ExactSizeIterator<Item = &'s Self::State>,
    {
        Err(invalid("map_agg requires its real emission context"))
    }
    fn build_final<'s, I>(
        &self,
        _states: I,
        _control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        Self::State: 's,
        I: ExactSizeIterator<Item = &'s Self::State>,
    {
        Err(invalid("map_agg requires its real emission context"))
    }
    fn prepare_update_evaluation<'a>(
        &'a self,
        input: SelectedAggregateUpdateInput<'a, 'a>,
        mapping: &[usize],
        allocator: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedUpdateBatch<'a>, EvaluationFailure> {
        observed(control, |work| {
            if !std::ptr::eq(input.contract(), self.contract.as_ref())
                || !self.contract.phase().consumes_logical_arguments()
                || input.logical_arguments().len() != 2
                || !input.order_arguments().is_empty()
            {
                return Err(
                    invalid("map_agg update differs from its checked phase or channels").into(),
                );
            }
            work.step()?;
            let (allocator, mapping) = mapped_domain(mapping, input.selection(), allocator, work)?;
            Ok(MapUpdate {
                input,
                mapping,
                allocator,
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
        observed(control, |work| {
            if !std::ptr::eq(input.contract(), self.contract.as_ref())
                || self.contract.phase().consumes_logical_arguments()
                || input.state().array().data_type()
                    != self.contract.intermediate_type().data_type()
            {
                return Err(invalid(
                    "map_agg merge differs from its checked phase or state carrier",
                )
                .into());
            }
            work.step()?;
            let (allocator, mapping) = mapped_domain(mapping, input.selection(), allocator, work)?;
            Ok(MapMerge {
                input,
                mapping,
                allocator,
            })
        })
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
        let result = observed(control, |work| {
            let input = &prepared.input;
            let row = input
                .selection()
                .row(ordinal)
                .ok_or_else(|| invalid("map_agg selected ordinal is absent"))?;
            work.step()?;
            let keys = input.logical_arguments()[0];
            let values = input.logical_arguments()[1];
            let kr = keys.value_row(ordinal, row);
            let vr = values.value_row(ordinal, row);
            if kr >= keys.array().len() || vr >= values.array().len() {
                return Err(internal("map_agg selected address is out of bounds").into());
            }
            let mut sink = FailureSink {
                allocator: &prepared.allocator,
                domain: FailureDomain::Mutation {
                    contract: &self.contract,
                    phase: AggregateInvocationPhase::Update,
                    selection: input.selection(),
                    mapping: &prepared.mapping,
                },
                control,
            };
            let from = state.core.entries.len();
            let mut scalar_work = ScalarWork::new(Some(work));
            let key_bytes = &mut state.key_bytes;
            let mut declarations = self.contract.logical_argument_types();
            let key_type = declarations
                .next()
                .expect("checked MAP_AGG key")
                .value_type();
            let value_type = declarations
                .next()
                .expect("checked MAP_AGG value")
                .value_type();
            core::update_from_arrays_observed(
                &mut state.core,
                (keys.array(), kr, Some(key_type)),
                (values.array(), vr, Some(value_type)),
                &mut scalar_work,
                &mut |capacity| {
                    *key_bytes = key_bytes
                        .checked_add(capacity)
                        .ok_or(KernelFailure::ResourceExhausted)?;
                    Ok(())
                },
            )
            .map_err(|error| sink.scalar(error))?;
            state
                .account_new_values(from, &mut scalar_work)
                .map_err(|error| sink.scalar(error))?;
            Ok(())
        });
        if result.is_err() {
            state.latch();
        }
        result
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
        let result = observed(control, |work| {
            let input = &prepared.input;
            let row = input
                .selection()
                .row(ordinal)
                .ok_or_else(|| invalid("map_agg selected merge ordinal is absent"))?;
            work.step()?;
            let argument = input.state();
            let address = argument.value_row(ordinal, row);
            if address >= argument.array().len() {
                return Err(internal("map_agg selected merge address is out of bounds").into());
            }
            let mut sink = FailureSink {
                allocator: &prepared.allocator,
                domain: FailureDomain::Mutation {
                    contract: &self.contract,
                    phase: AggregateInvocationPhase::Merge,
                    selection: input.selection(),
                    mapping: &prepared.mapping,
                },
                control,
            };
            let from = state.core.entries.len();
            let mut scalar_work = ScalarWork::new(Some(work));
            let merge = core::merge_input(argument.array()).map_err(|error| sink.scalar(error))?;
            let key_bytes = &mut state.key_bytes;
            core::merge_row_observed(
                &mut state.core,
                &merge,
                address,
                Some(self.contract.intermediate_type().value_type()),
                &mut scalar_work,
                &mut |capacity| {
                    *key_bytes = key_bytes
                        .checked_add(capacity)
                        .ok_or(KernelFailure::ResourceExhausted)?;
                    Ok(())
                },
            )
            .map_err(|error| sink.scalar(error))?;
            state
                .account_new_values(from, &mut scalar_work)
                .map_err(|error| sink.scalar(error))?;
            Ok(())
        });
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
        I: ExactSizeIterator<Item = &'s Self::State>,
    {
        self.emit(states, context, control, false)
    }
    fn build_final_evaluation_with_context<'s, I>(
        &self,
        states: I,
        context: &AggregateEmissionContext<'_>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, EvaluationFailure>
    where
        Self::State: 's,
        I: ExactSizeIterator<Item = &'s Self::State>,
    {
        self.emit(states, context, control, true)
    }
}
impl MapKernel {
    fn emit<'s, I>(
        &self,
        states: I,
        context: &AggregateEmissionContext<'_>,
        control: &dyn KernelEvaluationControl,
        final_output: bool,
    ) -> Result<ArrayRef, EvaluationFailure>
    where
        I: ExactSizeIterator<Item = &'s MapState>,
    {
        observed(control, |work| {
            let expected = states.len();
            self.check_emit(context, expected)?;
            if final_output != self.contract.phase().produces_final_result() {
                return Err(invalid("map_agg output port differs from its actual phase").into());
            }
            let host =
                HostAggregateAllocator::try_new(Arc::clone(context.allocator().ok_or_else(
                    || invalid("map_agg emission requires its actual host allocator"),
                )?))?;
            let mut sink = FailureSink {
                allocator: &host,
                domain: FailureDomain::Emission(context),
                control,
            };
            let target = if final_output {
                self.contract.final_type()
            } else {
                self.contract.intermediate_type()
            };
            let mut scalar_work = ScalarWork::new(Some(work));
            let output = core::build_array_checked_states(
                target.data_type(),
                Some(target.value_type()),
                states.map(|state| {
                    if state.failed {
                        Err(KernelFailure::InstanceFailed)
                    } else {
                        Ok(&state.core)
                    }
                }),
                &mut scalar_work,
            )
            .map_err(|error| sink.scalar(error))?;
            if output.len() != expected {
                return Err(
                    internal("map_agg state iterator differs from its actual extent").into(),
                );
            }
            Ok(output)
        })
    }
}

pub(super) fn validate_contract(
    contract: &AggregateCallContract,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    let [
        FunctionArgumentType::Value(_key),
        FunctionArgumentType::Value(_value),
    ] = contract.call().selected().argument_types.as_ref()
    else {
        return Err(invalid(
            "map_agg requires its original two full value arguments",
        ));
    };
    let FunctionResultType::Scalar(output) = &contract.call().selected().result_type else {
        return Err(invalid("map_agg requires its original scalar result"));
    };
    // The original resolver already owns the nested Map fields and all metadata.
    // No reader-support preflight is lawful: NULLs can skip unsupported children.
    if !matches!(output.data_type(), DataType::Map(..))
        || !output.exactly_equals_observed::<KernelFailure>(contract.intermediate_type(), || {
            work.step().map_err(compile_failure)
        })?
    {
        return Err(invalid(
            "map_agg selected result and state differ from the original exact Map signature",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "aggregate_map_tests.rs"]
mod tests;
