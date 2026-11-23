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

//! Selected host consumer of the ONE original ApproxTopK computation.
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_invocation_backing::HostDiagnostic;
use crate::aggregate_scalar::{self as scalar, ScalarWork};
use crate::approx_top_k_core::{self as core, TopKArgument};
use crate::array_backing_geometry::{self, BorrowedSourceNode, BorrowedSourceObservation};
use crate::kernel_control::{compile_failure, internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::opaque_memory::OpaqueRetainedCharge;
use crate::scalar_output_operation;
use crate::scalar_output_resources::ScalarOutputResources;
use crate::*;
use allocator_api2::vec::Vec as HostVec;
use arrow_array::{Array, ArrayRef, BinaryArray};
use arrow_schema::DataType;
use novarocks_type_contract::CompileCheckpoints;
use std::sync::Arc;
#[derive(Debug)]
pub(super) struct TopKKernel {
    pub(super) contract: Arc<AggregateCallContract>,
}
pub(super) struct TopKState {
    core: core::ApproxTopKState<HostAggregateAllocator>,
    failed: bool,
}
impl TopKState {
    fn latch(&mut self) {
        self.failed = true;
        // Destroy all state-owned blocks through their original actual allocator.
        self.core = core::ApproxTopKState::new(self.core.allocator.clone());
    }
}
pub(super) struct TopKUpdate<'a> {
    input: SelectedAggregateUpdateInput<'a, 'a>,
    mapping: HostVec<usize, HostAggregateAllocator>,
    allocator: HostAggregateAllocator,
    host: Arc<dyn AggregateStateAllocator>,
}
pub(super) struct TopKMerge<'a> {
    input: SelectedAggregateMergeInput<'a, 'a>,
    mapping: HostVec<usize, HostAggregateAllocator>,
    allocator: HostAggregateAllocator,
    host: Arc<dyn AggregateStateAllocator>,
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
                _ => return invalid("approx_top_k emission diagnostic has a foreign phase").into(),
            },
            _ => return invalid("approx_top_k mutation diagnostic has a foreign phase").into(),
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
            "approx_top_k mapping differs from actual selection",
        ));
    }
    let allocator = HostAggregateAllocator::try_new(
        host.ok_or_else(|| invalid("exact approx_top_k requires its host allocator"))?,
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

fn plus(a: usize, b: usize) -> Result<usize, KernelFailure> {
    a.checked_add(b).ok_or(KernelFailure::ResourceExhausted)
}
fn times(a: usize, b: usize) -> Result<usize, KernelFailure> {
    a.checked_mul(b).ok_or(KernelFailure::ResourceExhausted)
}
/// Borrow the existing source geometry author, without decoding any payload.
/// Each physical child length bounds original scalar Vec slots, including Null
/// children that have no buffer; Boolean bits expand to native scalar slots.
struct InputExtent {
    slots: usize,
}
impl<'a> BorrowedSourceObservation<'a> for InputExtent {
    fn node(
        &mut self,
        node: BorrowedSourceNode<'a>,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure> {
        self.slots = plus(self.slots, node.array.len())?;
        work.step()
    }
}
fn input_operation_bytes(
    arguments: &[EvaluatedArgument<'_>],
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<usize, KernelFailure> {
    let mut bytes = 0;
    for arg in arguments {
        let array = arg.array();
        let mut extent = InputExtent { slots: 0 };
        let metadata = array_backing_geometry::observe_source_metadata(
            array.as_ref(),
            false,
            work,
            &mut extent,
        )?;
        work.flush()?;
        let stock = array.get_buffer_memory_size();
        work.flush()?;
        // Original parameter scalar clones: old+new capacity and nested Option slots.
        // Metadata also covers the original escaped Debug dtype diagnostic String.
        let slots = times(
            extent.slots,
            4 * size_of::<Option<scalar::AggScalarValue>>(),
        )?;
        bytes = plus(
            bytes,
            plus(slots, plus(times(stock, 4)?, times(metadata, 4)?)?)?,
        )?;
    }
    // This is the longest fixed original diagnostic/decimal text scratch envelope,
    // not an unconditional host grant. The SAME real host reserves it below.
    plus(bytes, 256)
}
fn state_resources(
    state: &TopKState,
    ty: &DataType,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ScalarOutputResources, KernelFailure> {
    let mut total = ScalarOutputResources::from_tracked::<HostAggregateAllocator>(ty, None, work)?;
    total.slots = 0;
    total.heap_bytes = 0;
    for entry in state.core.counts.values() {
        let actual = ScalarOutputResources::from_tracked(ty, entry.value.as_ref(), work)?;
        total.slots = plus(total.slots, actual.slots)?;
        total.heap_bytes = plus(total.heap_bytes, actual.heap_bytes)?;
    }
    // Each encoded scalar tag/header/Decimal256 textual payload is <=128 bytes
    // per actual scalar graph slot, plus its original variable heap. Two encoded
    // Vec generations and the materialized scalar can coexist. Existing builder
    // resource arithmetic then includes the original buffer growth/custody peak.
    total.heap_bytes = plus(times(total.heap_bytes, 2)?, times(total.slots, 128)?)?;
    total.rows = 1; // TopK always invokes the original builder, even for empty groups.
    Ok(total)
}
fn topk_error(origin: KernelFailure, sink: &mut FailureSink<'_, '_>) -> EvaluationFailure {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    if origin.failure_fact().class() != FailureClass::Data {
        return origin.into();
    }
    let phase = match &sink.domain {
        FailureDomain::Mutation { phase, .. } => *phase,
        FailureDomain::Emission(context) => context.phase(),
    };
    match sink.diagnostic(&origin) {
        EvaluationFailure::InvocationData(data) => data.with_whole_value_source(origin).into(),
        EvaluationFailure::Kernel(diagnostic) => {
            KernelFailure::from_domain(TopKDiagnosticFailure {
                origin,
                _diagnostic: diagnostic,
                phase,
            })
            .into()
        }
    }
}
#[derive(Debug)]
struct TopKDiagnosticFailure {
    origin: KernelFailure,
    _diagnostic: KernelFailure,
    phase: AggregateInvocationPhase,
}
impl std::fmt::Display for TopKDiagnosticFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use crate::aggregate_format::AggregateFailureStage as Stage;
        let stage = match self.phase {
            AggregateInvocationPhase::Update => Stage::Update,
            AggregateInvocationPhase::Merge => Stage::Merge,
            AggregateInvocationPhase::Intermediate => Stage::BuildIntermediate,
            AggregateInvocationPhase::Final => Stage::BuildFinal,
        };
        stage.message(&self.origin).fmt(f)
    }
}
impl std::error::Error for TopKDiagnosticFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.origin)
    }
}
impl novarocks_failure_contract::FailureProjection for TopKDiagnosticFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureProjection::failure_fact(&self.origin)
    }
}
impl novarocks_failure_contract::OuterFailureProjection for TopKDiagnosticFailure {}

impl TopKKernel {
    fn output_type(&self) -> &DataType {
        self.contract.final_type().data_type()
    }
    fn emit<'s, I>(
        &self,
        states: I,
        context: &AggregateEmissionContext<'_>,
        control: &dyn KernelEvaluationControl,
        final_output: bool,
    ) -> Result<ArrayRef, EvaluationFailure>
    where
        I: ExactSizeIterator<Item = &'s TopKState>,
    {
        let expected = states.len();
        let mut states = states.peekable();
        // Existing failed states reject before this owner opens a scope/host loan.
        if states.peek().is_some_and(|state| state.failed) {
            return Err(KernelFailure::InstanceFailed.into());
        }
        observed(control, |work| {
            if !Arc::ptr_eq(context.contract(), &self.contract)
                || context.state_indices().len() != expected
            {
                return Err(invalid(
                    "approx_top_k emission differs from its actual contract or state order",
                )
                .into());
            }
            let host = Arc::clone(context.allocator().ok_or_else(|| {
                invalid("approx_top_k emission requires its actual host allocator")
            })?);
            let allocator = HostAggregateAllocator::try_new(Arc::clone(&host))?;
            let mut refs = HostVec::new_in(allocator.clone());
            if expected > 0 {
                work.flush()?;
                refs.try_reserve_exact(expected)
                    .map_err(|_| allocator.take_failure())?;
                work.flush()?;
            }
            let ty = if final_output {
                self.output_type()
            } else {
                &DataType::Binary
            };
            let mut resources = ScalarOutputResources::from_tracked::<HostAggregateAllocator>(
                ty,
                None,
                &mut ScalarWork::new(Some(work)),
            )?;
            resources.slots = 0;
            resources.heap_bytes = 0;
            resources.rows = expected.max(1);
            for state in states {
                if state.failed {
                    return Err(KernelFailure::InstanceFailed.into());
                }
                if refs.len() >= expected {
                    return Err(internal(
                        "approx_top_k emission iterator exceeded its actual extent",
                    )
                    .into());
                }
                let actual = state_resources(state, ty, &mut ScalarWork::new(Some(work)))?;
                resources.slots = plus(resources.slots, actual.slots)?;
                resources.heap_bytes = plus(resources.heap_bytes, actual.heap_bytes)?;
                refs.push(&state.core);
                work.step()?;
            }
            if refs.len() != expected {
                return Err(
                    internal("approx_top_k emission iterator shortened its actual extent").into(),
                );
            }
            let result = scalar_output_operation::with_scalar_output_operation(
                resources,
                host,
                &allocator,
                work,
                |scalar_work| {
                    if final_output {
                        core::output_topk_array_observed(
                            self.output_type(),
                            refs.iter().copied(),
                            scalar_work,
                        )
                    } else {
                        let mut builder = scalar::BinaryResultBuilder::new();
                        for state in &refs {
                            scalar_work.step()?;
                            let encoded = core::serialize_state_observed(state, scalar_work)?;
                            scalar_work.flush()?;
                            builder.append_value(&encoded)?;
                            scalar_work.flush()?;
                        }
                        let result = Arc::new(builder.finish()?) as ArrayRef;
                        scalar_work.flush()?;
                        Ok(result)
                    }
                },
            );
            match result {
                Ok(leased) => Ok(leased.values),
                Err(cause) => Err(topk_error(
                    cause,
                    &mut FailureSink {
                        allocator: &allocator,
                        domain: FailureDomain::Emission(context),
                        control,
                    },
                )),
            }
        })
    }
}
impl PreparedAggregateKernel for TopKKernel {
    type State = TopKState;
    type PreparedUpdateBatch<'a> = TopKUpdate<'a>;
    type PreparedMergeBatch<'a> = TopKMerge<'a>;
    fn contract(&self) -> &Arc<AggregateCallContract> {
        &self.contract
    }
    fn memory_policy(&self) -> AggregateStateMemoryPolicy {
        AggregateStateMemoryPolicy::AllocationTracked
    }
    // All containers and payload descendants use this host allocator directly.
    fn retained_bytes(&self, _state: &TopKState) -> usize {
        0
    }
    fn has_invocation_data(&self) -> bool {
        true
    }
    fn requires_emission_context(&self) -> bool {
        true
    }
    fn requires_empty_update_preparation(&self) -> bool {
        true
    }
    fn supports_local_stages(&self) -> bool {
        true
    }
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
        validate_contract(&contract, &mut work)?;
        work.finish().map_err(compile_failure)?;
        Ok(Arc::new(Self { contract }))
    }
    fn create_state(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<TopKState, KernelFailure> {
        control.checkpoint(0)?;
        Err(invalid("approx_top_k requires its actual host allocator"))
    }
    fn create_state_with_allocator(
        &self,
        host: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<TopKState, KernelFailure> {
        control.checkpoint(0)?;
        let host =
            host.ok_or_else(|| invalid("approx_top_k requires its actual host allocator"))?;
        OpaqueRetainedCharge::try_new(Arc::clone(&host))?;
        let allocator = HostAggregateAllocator::try_new(host)?;
        Ok(TopKState {
            core: core::ApproxTopKState::new(allocator),
            failed: false,
        })
    }
    fn prepare_update<'a>(
        &'a self,
        _input: SelectedAggregateUpdateInput<'a, 'a>,
        _control: &dyn KernelEvaluationControl,
    ) -> Result<TopKUpdate<'a>, KernelFailure> {
        Err(invalid("approx_top_k requires its lossless update port"))
    }
    fn update_row<'a>(
        &self,
        _state: &mut TopKState,
        _input: &TopKUpdate<'a>,
        _ordinal: usize,
        _control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        Err(invalid("approx_top_k requires its lossless update port"))
    }
    fn prepare_merge<'a>(
        &'a self,
        _input: SelectedAggregateMergeInput<'a, 'a>,
        _control: &dyn KernelEvaluationControl,
    ) -> Result<TopKMerge<'a>, KernelFailure> {
        Err(invalid("approx_top_k requires its lossless merge port"))
    }
    fn merge_row<'a>(
        &self,
        _state: &mut TopKState,
        _input: &TopKMerge<'a>,
        _ordinal: usize,
        _control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        Err(invalid("approx_top_k requires its lossless merge port"))
    }
    fn build_intermediate<'s, I>(
        &self,
        _states: I,
        _control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'s TopKState>,
    {
        Err(invalid("approx_top_k requires its actual emission context"))
    }
    fn build_final<'s, I>(
        &self,
        _states: I,
        _control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'s TopKState>,
    {
        Err(invalid("approx_top_k requires its actual emission context"))
    }
    fn prepare_update_evaluation<'a>(
        &'a self,
        input: SelectedAggregateUpdateInput<'a, 'a>,
        mapping: &[usize],
        host: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<TopKUpdate<'a>, EvaluationFailure> {
        observed(control, |work| {
            if !std::ptr::eq(input.contract(), self.contract.as_ref())
                || !self.contract.phase().consumes_logical_arguments()
                || input.logical_arguments().len() != self.contract.call().logical_argument_count()
                || !input.order_arguments().is_empty()
            {
                return Err(
                    invalid("approx_top_k update differs from its exact logical channels").into(),
                );
            }
            let actual = host
                .clone()
                .ok_or_else(|| invalid("approx_top_k update requires its actual host allocator"))?;
            let (allocator, mapping) = mapped_domain(mapping, input.selection(), host, work)?;
            if input.logical_arguments().len() == 1
                && let Some(recipe) =
                    core::update_input_failure(input.logical_arguments()[0].array())
            {
                return Err(topk_error(
                    KernelFailure::from_domain(recipe),
                    &mut FailureSink {
                        allocator: &allocator,
                        domain: FailureDomain::Mutation {
                            contract: &self.contract,
                            phase: AggregateInvocationPhase::Update,
                            selection: input.selection(),
                            mapping: &mapping,
                        },
                        control,
                    },
                ));
            }
            Ok(TopKUpdate {
                input,
                mapping,
                allocator,
                host: actual,
            })
        })
    }
    fn prepare_merge_evaluation<'a>(
        &'a self,
        input: SelectedAggregateMergeInput<'a, 'a>,
        mapping: &[usize],
        host: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<TopKMerge<'a>, EvaluationFailure> {
        observed(control, |work| {
            if !std::ptr::eq(input.contract(), self.contract.as_ref())
                || self.contract.phase().consumes_logical_arguments()
                || input.state().array().data_type() != &DataType::Binary
            {
                return Err(
                    invalid("approx_top_k merge differs from its exact state channel").into(),
                );
            }
            let actual = host
                .clone()
                .ok_or_else(|| invalid("approx_top_k merge requires its actual host allocator"))?;
            let (allocator, mapping) = mapped_domain(mapping, input.selection(), host, work)?;
            Ok(TopKMerge {
                input,
                mapping,
                allocator,
                host: actual,
            })
        })
    }
    fn update_row_evaluation<'a>(
        &self,
        state: &mut TopKState,
        prepared: &TopKUpdate<'a>,
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
                .ok_or_else(|| invalid("approx_top_k selected ordinal is absent"))?;
            let args = input.logical_arguments();
            let mut channels = [TopKArgument {
                array: args[0].array(),
                row: 0,
            }; 3];
            for (i, arg) in args.iter().enumerate() {
                let address = arg.value_row(ordinal, row);
                if address >= arg.array().len() {
                    return Err(internal("approx_top_k selected address is out of bounds").into());
                }
                channels[i] = TopKArgument {
                    array: arg.array(),
                    row: address,
                };
                work.step()?;
            }
            let bytes = input_operation_bytes(args, work)?;
            let charge = OpaqueRetainedCharge::try_new(Arc::clone(&prepared.host))?;
            work.flush()?;
            let reservation = charge.reserve_operation(bytes)?;
            work.flush()?;
            let result = core::update_arguments_observed(
                &mut state.core,
                &channels[..args.len()],
                &mut ScalarWork::new(Some(work)),
            );
            let result = match result {
                Ok(()) => Ok(()),
                Err(failure) => Err(topk_error(
                    failure,
                    &mut FailureSink {
                        allocator: &prepared.allocator,
                        domain: FailureDomain::Mutation {
                            contract: &self.contract,
                            phase: AggregateInvocationPhase::Update,
                            selection: input.selection(),
                            mapping: &prepared.mapping,
                        },
                        control,
                    },
                )),
            };
            drop(reservation);
            result
        });
        if result.is_err() {
            state.latch();
        }
        result
    }
    fn merge_row_evaluation<'a>(
        &self,
        state: &mut TopKState,
        prepared: &TopKMerge<'a>,
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
                .ok_or_else(|| invalid("approx_top_k selected merge ordinal is absent"))?;
            let arg = input.state();
            let address = arg.value_row(ordinal, row);
            if address >= arg.array().len() {
                return Err(internal("approx_top_k merge address is out of bounds").into());
            }
            let array = arg
                .array()
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| invalid("approx_top_k merge lost its exact Binary carrier"))?;
            if array.is_null(address) {
                return Ok(());
            }
            let bytes = input_operation_bytes(&[arg], work)?;
            let charge = OpaqueRetainedCharge::try_new(Arc::clone(&prepared.host))?;
            work.flush()?;
            let reservation = charge.reserve_operation(bytes)?;
            work.flush()?;
            let result = core::merge_payload_observed(
                &mut state.core,
                array.value(address),
                &mut ScalarWork::new(Some(work)),
            );
            let result = match result {
                Ok(()) => Ok(()),
                Err(failure) => Err(topk_error(
                    failure,
                    &mut FailureSink {
                        allocator: &prepared.allocator,
                        domain: FailureDomain::Mutation {
                            contract: &self.contract,
                            phase: AggregateInvocationPhase::Merge,
                            selection: input.selection(),
                            mapping: &prepared.mapping,
                        },
                        control,
                    },
                )),
            };
            drop(reservation);
            result
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
        I: ExactSizeIterator<Item = &'s TopKState>,
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
        I: ExactSizeIterator<Item = &'s TopKState>,
    {
        self.emit(states, context, control, true)
    }
}

pub(super) fn validate_contract(
    contract: &AggregateCallContract,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    validate_selected_profile(
        contract.call().selected(),
        contract.intermediate_type(),
        work,
    )
}
pub(super) fn validate_selected_profile(
    selected: &FunctionBindingSelection,
    intermediate: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    for arg in &selected.argument_types {
        let FunctionArgumentType::Value(_) = arg else {
            return Err(invalid(
                "approx_top_k requires original full value arguments",
            ));
        };
        work.step().map_err(compile_failure)?;
    }
    if !(1..=3).contains(&selected.argument_types.len()) {
        return Err(invalid(
            "approx_top_k requires original one to three full value arguments",
        ));
    }
    let FunctionResultType::Scalar(actual) = &selected.result_type else {
        return Err(invalid("approx_top_k requires its original scalar result"));
    };
    if !crate::aggregate_types::matches_selected_top_k_types_observed(
        &selected.argument_types,
        actual,
        intermediate,
        work,
    )
    .map_err(|error| match error {
        crate::FunctionBindingError::Control(error) => compile_failure(error),
        _ => invalid("approx_top_k original signature inference failed"),
    })? {
        return Err(invalid(
            "approx_top_k selected output or state differs from its original full signature",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "aggregate_top_k_tests.rs"]
mod tests;
