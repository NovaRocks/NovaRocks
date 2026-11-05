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

//! Complete four-identity DS HLL aggregate consumer of the original shared core.
//! Registration remains disabled until actual host and lifecycle probes pass.
use super::aggregate_ds_hll_core::{self as core, TypedDsHllStateAccess, TypedDsHllStorage};
use super::aggregate_ds_hll_failure::*;
use super::aggregate_ds_hll_state::{
    self as state_core, DsHllRetainedOperation, DsHllRetainedPort,
};
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_invocation_backing::HostDiagnostic;
use crate::datasketches_hll::{HllAccumulator, HllTargetType};
use crate::datasketches_hll_failure::*;
use crate::kernel_control::{compile_failure, internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::opaque_memory::{OpaqueReservation, OpaqueRetainedCharge};
use crate::*;
use allocator_api2::vec::Vec as HostVec;
use arrow_array::{Array, ArrayRef};
use arrow_schema::DataType;
use novarocks_type_contract::CompileCheckpoints;
use std::sync::Arc;
#[derive(Clone, Copy, Debug)]
pub(super) enum DsHllOperation {
    Hash,
    Count,
    Union,
}
#[derive(Debug)]
pub(super) struct DsHllKernel {
    pub(super) contract: Arc<AggregateCallContract>,
    pub(super) operation: DsHllOperation,
}
/// Fields deliberately destroy library state before releasing its real charge.
pub(super) struct DsHllState {
    accumulator: Option<HllAccumulator>,
    charge: OpaqueRetainedCharge,
    allocator: HostAggregateAllocator,
    failed: bool,
}
impl DsHllState {
    fn latch(&mut self) {
        self.accumulator = None;
        self.charge.release_retained();
        self.failed = true;
    }
}
impl<F: DsHllFailureSink<Error = EvaluationFailure>> DsHllRetainedPort<F> for OpaqueRetainedCharge {
    type Reservation = OpaqueReservation;
    fn payload_error_headroom(
        &self,
        preflight: crate::datasketches_hll::HllPayloadPreflight,
    ) -> usize {
        preflight.library_error_headroom_bytes()
    }
    fn reserve(
        &self,
        bytes: usize,
        _: DsHllRetainedOperation,
        sink: &mut F,
    ) -> Result<OpaqueReservation, EvaluationFailure> {
        self.reserve_operation(bytes)
            .map_err(|cause| sink.kernel(cause))
    }
    fn reconcile(
        &mut self,
        bytes: usize,
        reservation: &mut OpaqueReservation,
        sink: &mut F,
    ) -> Result<(), EvaluationFailure> {
        self.reconcile_under_reservation(bytes, reservation)
            .map_err(|cause| sink.kernel(cause))
    }
}
impl<F: DsHllFailureSink<Error = EvaluationFailure>> TypedDsHllStorage<F> for DsHllState {
    type Allocator = HostAggregateAllocator;
    fn allocator(&self) -> HostAggregateAllocator {
        self.allocator.clone()
    }
    fn accumulator(&self) -> Option<&HllAccumulator> {
        self.accumulator.as_ref()
    }
    fn ensure_accumulator(
        &mut self,
        lg: u8,
        target: HllTargetType,
        sink: &mut F,
    ) -> Result<(), EvaluationFailure> {
        state_core::ensure_accumulator(&mut self.accumulator, &mut self.charge, lg, target, sink)
            .map(|_| ())
    }
    fn update_hash(&mut self, hash: u64, sink: &mut F) -> Result<(), EvaluationFailure> {
        state_core::update_hash(&mut self.accumulator, &mut self.charge, hash, sink)
    }
    fn merge_payload(&mut self, payload: &[u8], sink: &mut F) -> Result<(), EvaluationFailure> {
        state_core::merge_payload(&mut self.accumulator, &mut self.charge, payload, sink)
    }
}
struct RowAccess<'a> {
    state: &'a mut DsHllState,
    row: usize,
}
impl<F: DsHllFailureSink<Error = EvaluationFailure>> TypedDsHllStateAccess<F> for RowAccess<'_> {
    type State = DsHllState;
    fn len(&self) -> usize {
        1
    }
    fn source_row(&self, _: usize) -> usize {
        self.row
    }
    fn with_state<T>(
        &mut self,
        _: usize,
        visit: impl FnOnce(&mut DsHllState) -> Result<T, EvaluationFailure>,
    ) -> Result<T, EvaluationFailure> {
        visit(self.state)
    }
}
pub(super) struct DsHllUpdate<'a> {
    input: SelectedAggregateUpdateInput<'a, 'a>,
    mapping: HostVec<usize, HostAggregateAllocator>,
    allocator: HostAggregateAllocator,
    host: Arc<dyn AggregateStateAllocator>,
}
pub(super) struct DsHllMerge<'a> {
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
    work: &'a mut EvaluationCheckpoints<'host>,
    host: &'a Arc<dyn AggregateStateAllocator>,
}
impl FailureSink<'_, '_> {
    fn diagnostic<T: std::fmt::Display + ?Sized>(
        &mut self,
        original: &T,
        reason: crate::DataFailureReason,
    ) -> EvaluationFailure {
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
                _ => return invalid("DS HLL emission diagnostic has a foreign phase").into(),
            },
            _ => return invalid("DS HLL mutation diagnostic has a foreign phase").into(),
        };
        // Recipe detection is not a published Data capsule. The following is
        // real host-backed original batch diagnostic construction work.
        let work = &mut *self.work;
        let result = (|| {
            let text = HostDiagnostic::prepare(self.allocator, work, |writer| {
                write!(writer, "{}", stage.message(original))
            })?;
            match &self.domain {
                FailureDomain::Mutation {
                    contract,
                    phase,
                    selection,
                    mapping,
                } => InvocationData::prepare_aggregate_with_reason(
                    self.allocator,
                    Arc::clone(contract),
                    *phase,
                    *selection,
                    mapping,
                    text,
                    reason,
                    work,
                ),
                FailureDomain::Emission(context) => InvocationData::prepare_emission_with_reason(
                    self.allocator,
                    context,
                    text,
                    reason,
                    work,
                ),
            }
        })();
        match result {
            Ok(data) => EvaluationFailure::InvocationData(data),
            Err(cause) => cause.into(),
        }
    }
}

impl HllFailureSink for FailureSink<'_, '_> {
    type Error = EvaluationFailure;
    fn failure(&mut self, recipe: HllFailureRecipe) -> EvaluationFailure {
        match recipe.classify() {
            crate::datasketches_hll_failure::ClassifiedHllFailure::Value(value) => {
                self.diagnostic(&value, value.reason())
            }
            crate::datasketches_hll_failure::ClassifiedHllFailure::Outer(cause) => {
                KernelFailure::from_domain(cause).into()
            }
        }
    }
    fn invariant(&mut self, recipe: HllInvariantRecipe) -> EvaluationFailure {
        KernelFailure::from_domain(recipe).into()
    }
    fn observe(&mut self, event: HllObservation) -> Result<(), EvaluationFailure> {
        match event {
            HllObservation::Step => self.work.step(),
            HllObservation::OpaqueBoundary => self.work.flush(),
        }
        .map_err(Into::into)
    }
}
impl DsHllFailureSink for FailureSink<'_, '_> {
    type Temporary = OpaqueReservation;
    fn kernel(&mut self, cause: KernelFailure) -> EvaluationFailure {
        cause.into()
    }
    fn input(&mut self, recipe: DsHllInputRecipe<'_>) -> EvaluationFailure {
        KernelFailure::from_domain(recipe.into_failure()).into()
    }

    fn reserve_temporary(&mut self, bytes: usize) -> Result<OpaqueReservation, EvaluationFailure> {
        let charge = OpaqueRetainedCharge::try_new(Arc::clone(self.host))?;
        charge.reserve_operation(bytes).map_err(Into::into)
    }
}
fn observed<'control, T>(
    control: &'control dyn KernelEvaluationControl,
    operation: impl FnOnce(&mut EvaluationCheckpoints<'control>) -> Result<T, EvaluationFailure>,
) -> Result<T, EvaluationFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = operation(&mut work);
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
        return Err(invalid("DS HLL mapping differs from actual selection"));
    }
    let allocator = HostAggregateAllocator::try_new(
        host.ok_or_else(|| invalid("exact DS HLL requires its host allocator"))?,
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

struct OpaquePayload {
    payload: Vec<u8>,
    _charge: OpaqueRetainedCharge,
}
impl AsRef<[u8]> for OpaquePayload {
    fn as_ref(&self) -> &[u8] {
        &self.payload
    }
}
struct EmissionPort {
    host: Arc<dyn AggregateStateAllocator>,
    allocator: HostAggregateAllocator,
}
impl EmissionPort {
    fn payload(
        &mut self,
        accumulator: &HllAccumulator,
        sink: &mut FailureSink<'_, '_>,
    ) -> Result<OpaquePayload, EvaluationFailure> {
        sink.observe(HllObservation::OpaqueBoundary)?;
        let preflight = accumulator.serialization_allocation_preflight_observed(&mut || {
            sink.observe(HllObservation::Step)
        })?;
        sink.observe(HllObservation::OpaqueBoundary)?;
        let mut charge = OpaqueRetainedCharge::try_new(Arc::clone(&self.host))?;
        let mut lease = charge.reserve_operation(preflight.bounds().additional_headroom_bytes)?;
        let payload =
            accumulator.serialize_under_reservation_with_failure(&preflight, &lease, sink)?;
        charge.reconcile_under_reservation(payload.capacity(), &mut lease)?;
        Ok(OpaquePayload {
            payload,
            _charge: charge,
        })
    }
}
impl core::DsHllEmissionPort<FailureSink<'_, '_>> for EmissionPort {
    type Payload = OpaquePayload;
    fn empty_payload(
        &mut self,
        sink: &mut FailureSink<'_, '_>,
    ) -> Result<OpaquePayload, EvaluationFailure> {
        let mut state = DsHllState {
            accumulator: None,
            charge: OpaqueRetainedCharge::try_new(Arc::clone(&self.host))?,
            allocator: self.allocator.clone(),
            failed: false,
        };
        state_core::ensure_accumulator(
            &mut state.accumulator,
            &mut state.charge,
            core::DEFAULT_LOG_K,
            core::DEFAULT_TARGET_TYPE,
            sink,
        )?;
        self.payload(
            state
                .accumulator
                .as_ref()
                .expect("ds_hll accumulator initialized"),
            sink,
        )
    }
    fn serialize(
        &mut self,
        accumulator: &HllAccumulator,
        sink: &mut FailureSink<'_, '_>,
    ) -> Result<OpaquePayload, EvaluationFailure> {
        self.payload(accumulator, sink)
    }
    fn clone_empty(
        &mut self,
        empty: &OpaquePayload,
        sink: &mut FailureSink<'_, '_>,
    ) -> Result<OpaquePayload, EvaluationFailure> {
        let mut charge = OpaqueRetainedCharge::try_new(Arc::clone(&self.host))?;
        let mut lease = charge.reserve_operation(empty.payload.len())?;
        if !empty.payload.is_empty() {
            sink.observe(HllObservation::OpaqueBoundary)?;
        }
        let payload = empty.payload.clone();
        charge.reconcile_under_reservation(payload.capacity(), &mut lease)?;
        if !empty.payload.is_empty() {
            sink.observe(HllObservation::OpaqueBoundary)?;
        }
        Ok(OpaquePayload {
            payload,
            _charge: charge,
        })
    }
}
struct EmissionStates<'a>(HostVec<&'a DsHllState, HostAggregateAllocator>);
impl core::DsHllEmissionStates for EmissionStates<'_> {
    fn len(&self) -> usize {
        self.0.len()
    }
    fn accumulator(&self, ordinal: usize) -> Option<&HllAccumulator> {
        self.0[ordinal].accumulator.as_ref()
    }
}
impl DsHllKernel {
    fn emit<'s, I>(
        &self,
        states: I,
        context: &AggregateEmissionContext<'_>,
        control: &dyn KernelEvaluationControl,
        intermediate: bool,
    ) -> Result<ArrayRef, EvaluationFailure>
    where
        I: ExactSizeIterator<Item = &'s DsHllState>,
    {
        observed(control, |work| {
            if !Arc::ptr_eq(context.contract(), &self.contract)
                || context.state_indices().len() != states.len()
            {
                return Err(invalid(
                    "DS HLL emission differs from its actual contract/state order",
                )
                .into());
            }
            let host = Arc::clone(
                context
                    .allocator()
                    .ok_or_else(|| invalid("DS HLL emission requires its actual opaque host"))?,
            );
            let allocator = HostAggregateAllocator::try_new(Arc::clone(&host))?;
            let mut owned = HostVec::new_in(allocator.clone());
            if states.len() != 0 {
                work.flush()?;
                owned
                    .try_reserve_exact(states.len())
                    .map_err(|_| allocator.take_failure())?;
                work.flush()?;
            }
            for state in states {
                work.step()?;
                if state.failed {
                    return Err(KernelFailure::InstanceFailed.into());
                }
                owned.push(state);
            }
            let source = EmissionStates(owned);
            let output = if intermediate {
                self.contract.intermediate_type().data_type()
            } else {
                let FunctionResultType::Scalar(output) =
                    &self.contract.call().selected().result_type
                else {
                    return Err(invalid("DS HLL result is not scalar").into());
                };
                output.data_type()
            };
            let mut sink = FailureSink {
                allocator: &allocator,
                domain: FailureDomain::Emission(context),
                work,
                host: &host,
            };
            core::build_array_with_sink(
                output,
                &source,
                &mut EmissionPort {
                    host: Arc::clone(&host),
                    allocator: allocator.clone(),
                },
                &mut sink,
            )
        })
    }
}
impl PreparedAggregateKernel for DsHllKernel {
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
    type State = DsHllState;
    type PreparedUpdateBatch<'a> = DsHllUpdate<'a>;
    type PreparedMergeBatch<'a> = DsHllMerge<'a>;
    fn contract(&self) -> &Arc<AggregateCallContract> {
        &self.contract
    }
    fn memory_policy(&self) -> AggregateStateMemoryPolicy {
        AggregateStateMemoryPolicy::AllocationTracked
    }
    fn retained_bytes(&self, state: &DsHllState) -> usize {
        state.allocator.metadata_bytes() + state.charge.bytes()
    }
    fn prepared_update_retained_bytes(&self, p: &DsHllUpdate<'_>) -> usize {
        p.allocator.metadata_bytes() + p.mapping.capacity() * std::mem::size_of::<usize>()
    }
    fn prepared_merge_retained_bytes(&self, p: &DsHllMerge<'_>) -> usize {
        p.allocator.metadata_bytes() + p.mapping.capacity() * std::mem::size_of::<usize>()
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
    ) -> Result<DsHllState, KernelFailure> {
        control.checkpoint(0)?;
        Err(invalid("DS HLL requires its actual opaque host"))
    }
    fn create_state_with_allocator(
        &self,
        allocator: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<DsHllState, KernelFailure> {
        control.checkpoint(0)?;
        let host = allocator.ok_or_else(|| invalid("DS HLL requires its actual opaque host"))?;
        let charge = OpaqueRetainedCharge::try_new(Arc::clone(&host))?;
        let allocator = HostAggregateAllocator::try_new(host)?;
        Ok(DsHllState {
            accumulator: None,
            charge,
            allocator,
            failed: false,
        })
    }
    fn prepare_update<'a>(
        &'a self,
        _: SelectedAggregateUpdateInput<'a, 'a>,
        _: &dyn KernelEvaluationControl,
    ) -> Result<DsHllUpdate<'a>, KernelFailure> {
        Err(invalid("DS HLL requires its lossless update port"))
    }
    fn update_row<'a>(
        &self,
        _: &mut DsHllState,
        _: &DsHllUpdate<'a>,
        _: usize,
        _: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        Err(invalid("DS HLL requires its lossless update port"))
    }
    fn prepare_merge<'a>(
        &'a self,
        _: SelectedAggregateMergeInput<'a, 'a>,
        _: &dyn KernelEvaluationControl,
    ) -> Result<DsHllMerge<'a>, KernelFailure> {
        Err(invalid("DS HLL requires its lossless merge port"))
    }
    fn merge_row<'a>(
        &self,
        _: &mut DsHllState,
        _: &DsHllMerge<'a>,
        _: usize,
        _: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        Err(invalid("DS HLL requires its lossless merge port"))
    }
    fn build_intermediate<'s, I>(
        &self,
        _: I,
        _: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'s DsHllState>,
    {
        Err(invalid("DS HLL requires its actual emission context"))
    }
    fn build_final<'s, I>(
        &self,
        _: I,
        _: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'s DsHllState>,
    {
        Err(invalid("DS HLL requires its actual emission context"))
    }
    fn prepare_update_evaluation<'a>(
        &'a self,
        input: SelectedAggregateUpdateInput<'a, 'a>,
        mapping: &[usize],
        allocator: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<DsHllUpdate<'a>, EvaluationFailure> {
        observed(control, |work| {
            if !std::ptr::eq(input.contract(), self.contract.as_ref())
                || !self.contract.phase().consumes_logical_arguments()
                || input.logical_arguments().len()
                    != self.contract.call().selected().argument_types.len()
                || !input.order_arguments().is_empty()
            {
                return Err(invalid("DS HLL update differs from actual logical channels").into());
            }
            let host = allocator
                .as_ref()
                .map(Arc::clone)
                .ok_or_else(|| invalid("DS HLL preparation requires its actual host"))?;
            if host.opaque_allocation_host().is_none() {
                return Err(invalid("DS HLL preparation requires actual opaque authority").into());
            }
            let (allocator, mapping) = mapped_domain(mapping, input.selection(), allocator, work)?;
            Ok(DsHllUpdate {
                input,
                mapping,
                allocator,
                host,
            })
        })
    }
    fn prepare_merge_evaluation<'a>(
        &'a self,
        input: SelectedAggregateMergeInput<'a, 'a>,
        mapping: &[usize],
        allocator: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<DsHllMerge<'a>, EvaluationFailure> {
        observed(control, |work| {
            if !std::ptr::eq(input.contract(), self.contract.as_ref())
                || self.contract.phase().consumes_logical_arguments()
            {
                return Err(invalid("DS HLL merge differs from its actual state phase").into());
            }
            let host = allocator
                .as_ref()
                .map(Arc::clone)
                .ok_or_else(|| invalid("DS HLL preparation requires its actual host"))?;
            if host.opaque_allocation_host().is_none() {
                return Err(invalid("DS HLL preparation requires actual opaque authority").into());
            }
            let (allocator, mapping) = mapped_domain(mapping, input.selection(), allocator, work)?;
            Ok(DsHllMerge {
                input,
                mapping,
                allocator,
                host,
            })
        })
    }
    fn update_row_evaluation<'a>(
        &self,
        state: &mut DsHllState,
        p: &DsHllUpdate<'a>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), EvaluationFailure> {
        if state.failed {
            return Err(KernelFailure::InstanceFailed.into());
        }
        let result = observed(control, |work| {
            let batch_row = p
                .input
                .selection()
                .row(ordinal)
                .ok_or_else(|| invalid("DS HLL selected update ordinal is absent"))?;
            work.step()?;
            let arguments = p.input.logical_arguments();
            let channel = |index: usize| {
                let argument = arguments[index];
                core::DsHllChannel {
                    array: argument.array(),
                    row: argument.value_row(ordinal, batch_row),
                }
            };
            for argument in arguments {
                if argument.value_row(ordinal, batch_row) >= argument.array().len() {
                    return Err(internal("DS HLL evaluated update address is out of bounds").into());
                }
            }
            let first = channel(0);
            let mut access = RowAccess {
                state,
                row: first.row,
            };
            let mut sink = FailureSink {
                allocator: &p.allocator,
                domain: FailureDomain::Mutation {
                    contract: &self.contract,
                    phase: AggregateInvocationPhase::Update,
                    selection: p.input.selection(),
                    mapping: &p.mapping,
                },
                work,
                host: &p.host,
            };
            if arguments.len() == 1 {
                core::update_batch_with_sink(
                    match self.operation {
                        DsHllOperation::Hash => core::DsHllUpdateMode::Hash,
                        DsHllOperation::Count => core::DsHllUpdateMode::Count,
                        DsHllOperation::Union => core::DsHllUpdateMode::Merge,
                    },
                    first.array,
                    &mut access,
                    &mut sink,
                )
            } else {
                core::update_hash_channels_at(
                    first,
                    arguments.get(1).map(|_| channel(1)),
                    arguments.get(2).map(|_| channel(2)),
                    &mut access,
                    0,
                    DsHllInputContext::CountDistinct,
                    &mut sink,
                )
            }
        });
        if result.is_err() {
            state.latch();
        }
        result
    }
    fn merge_row_evaluation<'a>(
        &self,
        state: &mut DsHllState,
        p: &DsHllMerge<'a>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), EvaluationFailure> {
        if state.failed {
            return Err(KernelFailure::InstanceFailed.into());
        }
        let result = observed(control, |work| {
            let batch_row = p
                .input
                .selection()
                .row(ordinal)
                .ok_or_else(|| invalid("DS HLL selected merge ordinal is absent"))?;
            work.step()?;
            let argument = p.input.state();
            let row = argument.value_row(ordinal, batch_row);
            if row >= argument.array().len() {
                return Err(internal("DS HLL evaluated merge address is out of bounds").into());
            }
            let mut access = RowAccess { state, row };
            let mut sink = FailureSink {
                allocator: &p.allocator,
                domain: FailureDomain::Mutation {
                    contract: &self.contract,
                    phase: AggregateInvocationPhase::Merge,
                    selection: p.input.selection(),
                    mapping: &p.mapping,
                },
                work,
                host: &p.host,
            };
            core::merge_batch_with_sink(
                argument.array(),
                &mut access,
                DsHllInputContext::Merge,
                &mut sink,
            )
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
        I: ExactSizeIterator<Item = &'s DsHllState>,
    {
        self.emit(states, context, control, true)
    }
    fn build_final_evaluation_with_context<'s, I>(
        &self,
        states: I,
        context: &AggregateEmissionContext<'_>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, EvaluationFailure>
    where
        I: ExactSizeIterator<Item = &'s DsHllState>,
    {
        self.emit(states, context, control, false)
    }
}
pub(super) fn validate_contract(
    contract: &AggregateCallContract,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    for argument in contract.call().selected().argument_types.iter() {
        work.step().map_err(compile_failure)?;
        if !matches!(argument, FunctionArgumentType::Value(_)) {
            return Err(invalid("DS HLL requires actual value arguments"));
        }
    }
    let FunctionResultType::Scalar(output) = &contract.call().selected().result_type else {
        return Err(invalid("DS HLL requires its original scalar result"));
    };
    if !matches!(output.data_type(), DataType::Int64 | DataType::Binary)
        || contract.intermediate_type().data_type() != &DataType::Binary
    {
        return Err(invalid(
            "DS HLL selected result or state differs from its original profile",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "aggregate_ds_hll_tests.rs"]
mod tests;
