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

//! Selected consumer of the sole original TDigest/row/codec authors.
//! Original temporary String/Vec/Arrow work remains opaque; full Data is
//! published only after the actual host-backed diagnostic has been built.
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_invocation_backing::HostDiagnostic;
use crate::aggregate_scalar::{self as scalar, ScalarStateError, ScalarWork};
use crate::approx_percentile_aggregate_core::{
    self as core, ApproxPercentileDiagnostic as Diagnostic,
};
use crate::approx_percentile_core as digest;
use crate::kernel_control::{compile_failure, internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::*;
use allocator_api2::vec::Vec as HostVec;
use arrow_array::{ArrayRef, builder::BinaryBuilder};
use arrow_schema::DataType;
use novarocks_type_contract::CompileCheckpoints;
use std::sync::Arc;
#[derive(Clone, Copy, Debug)]
pub(super) enum ApproxPercentileOperation {
    Unweighted,
    Weighted,
    Union,
}
impl ApproxPercentileOperation {
    pub(super) fn accepts_arity(self, count: usize) -> bool {
        match self {
            Self::Unweighted => matches!(count, 2 | 3),
            Self::Weighted => matches!(count, 3 | 4),
            Self::Union => count == 1,
        }
    }
    fn update(self) -> Diagnostic {
        match self {
            Self::Unweighted | Self::Union => Diagnostic::UnweightedUpdate,
            Self::Weighted => Diagnostic::WeightedUpdate,
        }
    }
    fn merge(self) -> Diagnostic {
        match self {
            Self::Unweighted | Self::Union => Diagnostic::UnweightedMerge,
            Self::Weighted => Diagnostic::WeightedMerge,
        }
    }
}
#[derive(Debug)]
pub(super) struct ApproxPercentileKernel {
    pub(super) contract: Arc<AggregateCallContract>,
    pub(super) operation: ApproxPercentileOperation,
}
pub(super) struct ApproxPercentileState {
    core: digest::PercentileState<HostAggregateAllocator>,
    failed: bool,
}
impl ApproxPercentileState {
    fn latch(&mut self) {
        self.failed = true;
        self.core = digest::PercentileState::new_in(
            digest::DEFAULT_COMPRESSION_FACTOR,
            self.core.allocator(),
        );
    }
}
pub(super) struct ApproxPercentileUpdate<'a> {
    input: SelectedAggregateUpdateInput<'a, 'a>,
    mapping: HostVec<usize, HostAggregateAllocator>,
    allocator: HostAggregateAllocator,
    row_input: UpdateChannels,
}
enum UpdateChannels {
    Unweighted(core::UnweightedInput),
    Weighted(core::WeightedInput),
    Payload(ArrayRef),
}
pub(super) struct ApproxPercentileMerge<'a> {
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
                _ => {
                    return invalid(
                        "approximate percentile emission diagnostic has a foreign phase",
                    )
                    .into();
                }
            },
            _ => {
                return invalid("approximate percentile mutation diagnostic has a foreign phase")
                    .into();
            }
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
    fn original(&mut self, error: String, allocator: &HostAggregateAllocator) -> EvaluationFailure {
        // Only an actual recorded host refusal may replace its temporary
        // original String carrier. Never classify error text as a resource.
        match allocator.take_recorded_failure() {
            Some(cause) => cause.into(),
            None => self.diagnostic(&error),
        }
    }
    fn scalar(&mut self, error: ScalarStateError) -> EvaluationFailure {
        match error {
            ScalarStateError::Kernel(cause) => cause.into(),
            ScalarStateError::OutputAllocation(_) => KernelFailure::ResourceExhausted.into(),
            ScalarStateError::Legacy(message) => self.diagnostic(&message),
        }
    }
}
// The observer/sink borrows the actual phase receipt and actual allocators.
// It never classifies a diagnostic string as a Kernel cause.
struct ObservedFailureSink<'a, 'host, 'control> {
    failure: FailureSink<'a, 'host>,
    state_allocator: HostAggregateAllocator,
    work: &'a mut EvaluationCheckpoints<'control>,
}
impl crate::approx_percentile_failure::ApproxPercentileFailureSink
    for ObservedFailureSink<'_, '_, '_>
{
    type Error = EvaluationFailure;
    fn data(
        &mut self,
        recipe: crate::approx_percentile_failure::ApproxPercentileDataRecipe<'_>,
    ) -> EvaluationFailure {
        self.failure.diagnostic(&recipe)
    }
    fn allocation(
        &mut self,
        _recipe: crate::approx_percentile_failure::ApproxPercentileDataRecipe<'_>,
    ) -> EvaluationFailure {
        self.state_allocator
            .take_recorded_failure()
            .or_else(|| self.failure.allocator.take_recorded_failure())
            .unwrap_or(KernelFailure::ResourceExhausted)
            .into()
    }
    fn observe(
        &mut self,
        observation: crate::approx_percentile_failure::ApproxPercentileObservation,
    ) -> Result<(), EvaluationFailure> {
        use crate::approx_percentile_failure::ApproxPercentileObservation as O;
        match observation {
            O::Step => self.work.step(),
            O::SortBegin(_) | O::SortEnd | O::JsonBegin(_) | O::JsonEnd => self.work.flush(),
        }
        .map_err(EvaluationFailure::from)
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
        return Err(invalid(
            "approximate percentile mapping differs from actual selection",
        ));
    }
    let allocator = HostAggregateAllocator::try_new(
        host.ok_or_else(|| invalid("exact percentile requires its host allocator"))?,
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

impl ApproxPercentileKernel {
    fn output_type(&self) -> &DataType {
        let FunctionResultType::Scalar(output) = &self.contract.call().selected().result_type
        else {
            unreachable!("checked scalar result")
        };
        &output.data_type
    }
    fn check_emit(
        &self,
        context: &AggregateEmissionContext<'_>,
        count: usize,
    ) -> Result<(), KernelFailure> {
        if !Arc::ptr_eq(context.contract(), &self.contract)
            || context.state_indices().len() != count
        {
            return Err(invalid(
                "approximate percentile emission differs from actual contract/state order",
            ));
        }
        Ok(())
    }
}
impl PreparedAggregateKernel for ApproxPercentileKernel {
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
        }))
    }
    type State = ApproxPercentileState;
    type PreparedUpdateBatch<'a> = ApproxPercentileUpdate<'a>;
    type PreparedMergeBatch<'a> = ApproxPercentileMerge<'a>;
    fn contract(&self) -> &Arc<AggregateCallContract> {
        &self.contract
    }
    fn memory_policy(&self) -> AggregateStateMemoryPolicy {
        AggregateStateMemoryPolicy::AllocationTracked
    }
    fn retained_bytes(&self, state: &Self::State) -> usize {
        state.core.allocator().metadata_bytes()
            + state
                .core
                .retained_bytes()
                .saturating_sub(std::mem::size_of_val(&state.core))
    }
    fn has_invocation_data(&self) -> bool {
        true
    }
    fn requires_empty_update_preparation(&self) -> bool {
        // The original packed Struct validator runs before its row loop,
        // including an actually invoked zero-row Union update.
        matches!(self.operation, ApproxPercentileOperation::Union)
    }
    fn requires_emission_context(&self) -> bool {
        true
    }
    fn create_state(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        control.checkpoint(0)?;
        Err(invalid(
            "approximate percentile requires its actual host allocator",
        ))
    }
    fn create_state_with_allocator(
        &self,
        host: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        control.checkpoint(0)?;
        let allocator = HostAggregateAllocator::try_new(host.ok_or_else(|| {
            invalid("approximate percentile requires its actual host allocator")
        })?)?;
        Ok(ApproxPercentileState {
            core: digest::PercentileState::new_in(digest::DEFAULT_COMPRESSION_FACTOR, allocator),
            failed: false,
        })
    }
    fn prepare_update<'a>(
        &'a self,
        _input: SelectedAggregateUpdateInput<'a, 'a>,
        _control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedUpdateBatch<'a>, KernelFailure> {
        Err(invalid(
            "approximate percentile requires its lossless update port",
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
            "approximate percentile requires its lossless update port",
        ))
    }
    fn prepare_merge<'a>(
        &'a self,
        _input: SelectedAggregateMergeInput<'a, 'a>,
        _control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedMergeBatch<'a>, KernelFailure> {
        Err(invalid(
            "approximate percentile requires its lossless merge port",
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
            "approximate percentile requires its lossless merge port",
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
        Err(invalid(
            "approximate percentile requires its actual emission context",
        ))
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
        Err(invalid(
            "approximate percentile requires its actual emission context",
        ))
    }
    fn prepare_update_evaluation<'a>(
        &'a self,
        input: SelectedAggregateUpdateInput<'a, 'a>,
        mapping: &[usize],
        host: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedUpdateBatch<'a>, EvaluationFailure> {
        observed(control, |work| {
            if !std::ptr::eq(input.contract(), self.contract.as_ref())
                || !self.contract.phase().consumes_logical_arguments()
                || !self
                    .operation
                    .accepts_arity(input.logical_arguments().len())
                || !input.order_arguments().is_empty()
            {
                return Err(invalid(
                    "approximate percentile update differs from actual phase/channels",
                )
                .into());
            }
            work.step()?;
            let (allocator, mapping) = mapped_domain(mapping, input.selection(), host, work)?;
            let args = input.logical_arguments();
            // Only Arc/channel projection occurs here; no sample-dependent
            // rate/type/NULL/state validation runs before the original row.
            let row_input = match self.operation {
                ApproxPercentileOperation::Unweighted => {
                    UpdateChannels::Unweighted(core::UnweightedInput::from_arguments(
                        args[0].array().clone(),
                        args[1].array().clone(),
                        args.get(2).map(|arg| arg.array().clone()),
                    ))
                }
                ApproxPercentileOperation::Weighted => {
                    UpdateChannels::Weighted(core::WeightedInput::from_arguments(
                        args[0].array().clone(),
                        args[1].array().clone(),
                        args[2].array().clone(),
                        args.get(3).map(|arg| arg.array().clone()),
                    ))
                }
                ApproxPercentileOperation::Union => {
                    let array = args[0].array();
                    if let Some(structure) =
                        array.as_any().downcast_ref::<arrow_array::StructArray>()
                    {
                        let channels =
                            core::UnweightedInput::try_new(structure, self.operation.update())
                                .map_err(|original| {
                                    FailureSink {
                                        allocator: &allocator,
                                        domain: FailureDomain::Mutation {
                                            contract: &self.contract,
                                            phase: AggregateInvocationPhase::Update,
                                            selection: input.selection(),
                                            mapping: &mapping,
                                        },
                                        control,
                                    }
                                    .diagnostic(&original)
                                })?;
                        UpdateChannels::Unweighted(channels)
                    } else {
                        UpdateChannels::Payload(array.clone())
                    }
                }
            };
            work.step()?;
            Ok(ApproxPercentileUpdate {
                input,
                mapping,
                allocator,
                row_input,
            })
        })
    }
    fn prepare_merge_evaluation<'a>(
        &'a self,
        input: SelectedAggregateMergeInput<'a, 'a>,
        mapping: &[usize],
        host: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedMergeBatch<'a>, EvaluationFailure> {
        observed(control, |work| {
            if !std::ptr::eq(input.contract(), self.contract.as_ref())
                || self.contract.phase().consumes_logical_arguments()
                || input.state().array().data_type() != &DataType::Binary
            {
                return Err(invalid(
                    "approximate percentile merge differs from actual phase/state channel",
                )
                .into());
            }
            work.step()?;
            let (allocator, mapping) = mapped_domain(mapping, input.selection(), host, work)?;
            Ok(ApproxPercentileMerge {
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
            let input = prepared.input;
            let row = input
                .selection()
                .row(ordinal)
                .ok_or_else(|| invalid("approximate percentile selected ordinal is absent"))?;
            let mut addresses = [0usize; 4];
            for (index, arg) in input.logical_arguments().iter().enumerate() {
                addresses[index] = arg.value_row(ordinal, row);
                if addresses[index] >= arg.array().len() {
                    return Err(internal(
                        "approximate percentile selected address is outside actual channel",
                    )
                    .into());
                }
                work.step()?;
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
            // Observe original owned loops through the same core sink. Real
            // library sort boundaries remain opaque and receive no grant from
            // this observer; published Data has no success footer.
            work.flush()?;
            // Packed Union children inherit their sole parent address. The
            // original Struct reader intentionally ignores the root NULL mask.
            let packed = matches!(self.operation, ApproxPercentileOperation::Union);
            let allocator = state.core.allocator();
            let scratch = core::AllocatedQuantileScratch(prepared.allocator.clone());
            let mut observed_sink = ObservedFailureSink {
                failure: sink,
                state_allocator: allocator,
                work,
            };
            match &prepared.row_input {
                UpdateChannels::Payload(array) => core::merge_row_with_sink(
                    &mut state.core,
                    array,
                    addresses[0],
                    self.operation.update(),
                    &mut observed_sink,
                )?,
                UpdateChannels::Unweighted(channels) => channels.update_addresses_with_sink(
                    &mut state.core,
                    addresses[0],
                    if packed { addresses[0] } else { addresses[1] },
                    if packed { addresses[0] } else { addresses[2] },
                    self.operation.update(),
                    &scratch,
                    &mut observed_sink,
                )?,
                UpdateChannels::Weighted(channels) => channels.update_addresses_with_sink(
                    &mut state.core,
                    addresses[0],
                    addresses[1],
                    addresses[2],
                    addresses[3],
                    self.operation.update(),
                    &scratch,
                    &mut observed_sink,
                )?,
            }
            drop(observed_sink);
            work.flush()?;
            Ok(())
        });
        if result.is_err() {
            state.latch()
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
            let input = prepared.input;
            let row = input
                .selection()
                .row(ordinal)
                .ok_or_else(|| invalid("approximate percentile merge ordinal is absent"))?;
            let arg = input.state();
            let address = arg.value_row(ordinal, row);
            if address >= arg.array().len() {
                return Err(internal(
                    "approximate percentile merge address is outside actual channel",
                )
                .into());
            }
            work.step()?;
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
            work.flush()?;
            let allocator = state.core.allocator();
            let mut observed_sink = ObservedFailureSink {
                failure: sink,
                state_allocator: allocator,
                work,
            };
            core::merge_row_with_sink(
                &mut state.core,
                arg.array(),
                address,
                self.operation.merge(),
                &mut observed_sink,
            )?;
            drop(observed_sink);
            work.flush()?;
            Ok(())
        });
        if result.is_err() {
            state.latch()
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
        observed(control, |work| {
            let expected = states.len();
            self.check_emit(context, expected)?;
            let mut builder = BinaryBuilder::new();
            let mut count = 0;
            for state in states {
                if state.failed {
                    return Err(KernelFailure::InstanceFailed.into());
                }
                if count >= expected {
                    return Err(internal(
                        "approximate percentile state iterator exceeded actual extent",
                    )
                    .into());
                }
                work.step()?;
                work.flush()?;
                let encoded =
                    digest::encode_state_observed(
                        &state.core,
                        &mut |observation| match observation {
                            digest::PercentileEncodeObservation::Step => work.step(),
                            digest::PercentileEncodeObservation::OpaqueBegin
                            | digest::PercentileEncodeObservation::OpaqueEnd => work.flush(),
                        },
                    )?;
                work.flush()?;
                builder.append_value(encoded);
                work.flush()?;
                count += 1;
            }
            if count != expected {
                return Err(internal(
                    "approximate percentile state iterator shortened actual extent",
                )
                .into());
            }
            let array = Arc::new(builder.finish()) as ArrayRef;
            work.flush()?;
            Ok(array)
        })
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
        if matches!(self.operation, ApproxPercentileOperation::Union) {
            // Original Union Final and Intermediate share the exact state
            // encoding and Binary writer, with this real emission receipt.
            return self.build_intermediate_evaluation_with_context(states, context, control);
        }
        observed(control, |work| {
            let expected = states.len();
            self.check_emit(context, expected)?;
            let host =
                HostAggregateAllocator::try_new(Arc::clone(context.allocator().ok_or_else(
                    || invalid("approximate percentile final requires actual host allocator"),
                )?))?;
            let mut sink = FailureSink {
                allocator: &host,
                domain: FailureDomain::Emission(context),
                control,
            };
            let output_kind = match self.output_type() {
                DataType::Float64 => core::ScalarOutput::Float64,
                DataType::List(_) => core::ScalarOutput::List,
                _ => {
                    return Err(invalid(
                        "approximate percentile final carrier differs from original signature",
                    )
                    .into());
                }
            };
            let mut output = Vec::new();
            output
                .try_reserve_exact(expected)
                .map_err(|_| KernelFailure::ResourceExhausted)?;
            for state in states {
                if state.failed {
                    return Err(KernelFailure::InstanceFailed.into());
                }
                if output.len() >= expected {
                    return Err(internal(
                        "approximate percentile final iterator exceeded actual extent",
                    )
                    .into());
                }
                work.step()?;
                work.flush()?;
                let original = core::scalar_output_with_policy(
                    &state.core,
                    output_kind,
                    &mut digest::FallibleClone,
                );
                let value = match original {
                    Ok(value) => value,
                    Err(error) => return Err(sink.original(error, &state.core.allocator())),
                };
                work.flush()?;
                output.push(value);
                work.flush()?;
            }
            if output.len() != expected {
                return Err(internal(
                    "approximate percentile final iterator shortened actual extent",
                )
                .into());
            }
            let mut scalar_work = ScalarWork::new(Some(work));
            scalar::build_scalar_array(self.output_type(), output, &mut scalar_work)
                .map_err(|error| sink.scalar(error))
        })
    }
}
pub(super) fn validate_contract(
    contract: &AggregateCallContract,
    signature: &ResolvedAggregateSignature,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    let FunctionResultType::Scalar(output) = &contract.call().selected().result_type else {
        return Err(invalid(
            "approximate percentile requires original scalar result",
        ));
    };
    if !FunctionValueType::new(signature.output_type.clone(), true)
        .exactly_equals_observed::<KernelFailure>(output, || work.step().map_err(compile_failure))?
        || !FunctionValueType::new(signature.intermediate_type.clone(), true)
            .exactly_equals_observed::<KernelFailure>(contract.intermediate_type(), || {
                work.step().map_err(compile_failure)
            })?
    {
        return Err(invalid(
            "approximate percentile full return/state differ from original resolver",
        ));
    }
    Ok(())
}
#[cfg(test)]
#[path = "aggregate_approx_percentile_tests.rs"]
mod tests;
