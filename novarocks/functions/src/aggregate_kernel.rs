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

//! Pure typed aggregate preparation and selected update/merge lifecycles.
//! Execution owns group mapping, state arena/type erasure and destruction.

use crate::kernel_control::{internal, invalid};
use crate::kernel_input::{EvaluationCheckpoints, validate_argument_observed};
use crate::{
    AggregateCallContract, AggregateKernelPhase, AggregateOrderKey, AggregateStateMemoryPolicy,
    CallEffectInput, EvaluatedArgument, FunctionBindingError, FunctionBindingResolver,
    FunctionBindingSelection, FunctionCallContract, FunctionEffectOwner,
    FunctionSpecializationFailure, KernelEvaluationControl, KernelFailure, ScopedExpressionEffects,
    SelectedAggregateMergeInput, SelectedAggregateUpdateInput, Selection,
};
use arrow_array::ArrayRef;
use novarocks_type_contract::{CallEffects, CompilePhase, FunctionValueType, PureCompileControl};
use std::{alloc::Layout, fmt, sync::Arc};

/// A borrowed receipt from the actual host emission, never an update-row domain.
/// Capacity is an output-shape fact, not an allocation grant.
#[derive(Clone, Copy)]
pub struct AggregateEmissionContext<'host> {
    contract: &'host Arc<AggregateCallContract>,
    indices: &'host [usize],
    row_capacity: usize,
    allocator: Option<&'host Arc<dyn crate::AggregateStateAllocator>>,
}
impl<'host> AggregateEmissionContext<'host> {
    pub(crate) fn from_host(
        contract: &'host Arc<AggregateCallContract>,
        indices: &'host [usize],
        row_capacity: usize,
        allocator: Option<&'host Arc<dyn crate::AggregateStateAllocator>>,
    ) -> Self {
        Self {
            contract,
            indices,
            row_capacity,
            allocator,
        }
    }
    pub fn contract(&self) -> &'host Arc<AggregateCallContract> {
        self.contract
    }
    pub fn state_indices(&self) -> &'host [usize] {
        self.indices
    }
    pub fn row_capacity(&self) -> usize {
        self.row_capacity
    }
    /// Emission visits every supplied state in this exact output order.
    pub fn selection(&self) -> Selection<'static> {
        Selection::all(self.indices.len())
    }
    pub fn allocator(&self) -> Option<&'host Arc<dyn crate::AggregateStateAllocator>> {
        self.allocator
    }
    pub fn phase(&self) -> crate::AggregateInvocationPhase {
        if self.contract.phase().produces_final_result() {
            crate::AggregateInvocationPhase::Final
        } else {
            crate::AggregateInvocationPhase::Intermediate
        }
    }
}
impl fmt::Debug for AggregateEmissionContext<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AggregateEmissionContext")
            .field("contract", self.contract)
            .field("indices", &self.indices)
            .field("row_capacity", &self.row_capacity)
            .field("has_allocator", &self.allocator.is_some())
            .finish()
    }
}

/// Typed, immutable, exact preparation. The host monomorphizes batch dispatch;
/// this is not a per-row dynamic ABI or a private aggregate state arena.
///
/// State construction and every mutation/emission run inside host-installed
/// memory scopes with prior allocation/temporary-peak authorization. Retained
/// bounds below are facts for that owner, not grants or a second wallet.
/// Whole invocation Data is non-maskable and retains its complete diagnostic; control,
/// resource and internal failures keep their distinct outer categories.
pub trait PreparedAggregateKernel: Send + Sync + fmt::Debug + 'static {
    type State: Send + 'static;
    type PreparedUpdateBatch<'batch>
    where
        Self: 'batch;
    type PreparedMergeBatch<'batch>
    where
        Self: 'batch;

    fn contract(&self) -> &Arc<AggregateCallContract>;
    /// Explicit same-owner phase construction. The framework supplies only
    /// its authenticated local-stage contracts; semantic fields stay owned
    /// by this concrete implementation. No old contract pointer is reused.
    fn clone_for_local_phase(
        &self,
        _contract: Arc<AggregateCallContract>,
        _control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Arc<Self>, KernelFailure>
    where
        Self: Sized,
    {
        Err(invalid(
            "aggregate owner has no local-stage phase implementation",
        ))
    }
    fn memory_policy(&self) -> AggregateStateMemoryPolicy;
    fn create_state(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure>;
    /// Construct state with the host authority for owned heap allocations.
    fn create_state_with_allocator(
        &self,
        _allocator: Option<Arc<dyn crate::AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        if self.memory_policy() == AggregateStateMemoryPolicy::AllocationTracked {
            return Err(invalid(
                "allocation-tracked aggregate requires a host allocator implementation",
            ));
        }
        self.create_state(control)
    }
    /// Borrow only actually selected values; never inspect unused batch rows
    /// or advance group state while preparing the input view.
    fn prepare_update<'batch>(
        &'batch self,
        input: SelectedAggregateUpdateInput<'batch, 'batch>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedUpdateBatch<'batch>, KernelFailure>;
    /// The ordinal is relative to the exact prepared input's Selection.
    /// NULL and encoded carriers are read through that input's value_row map.
    fn update_row<'batch>(
        &self,
        state: &mut Self::State,
        prepared: &Self::PreparedUpdateBatch<'batch>,
        selected_ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure>;
    /// As with update preparation, only selected input values are reachable.
    fn prepare_merge<'batch>(
        &'batch self,
        input: SelectedAggregateMergeInput<'batch, 'batch>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedMergeBatch<'batch>, KernelFailure>;
    fn merge_row<'batch>(
        &self,
        state: &mut Self::State,
        prepared: &Self::PreparedMergeBatch<'batch>,
        selected_ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure>;
    fn build_intermediate<'state, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        Self::State: 'state,
        I: ExactSizeIterator<Item = &'state Self::State>;
    fn build_final<'state, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        Self::State: 'state,
        I: ExactSizeIterator<Item = &'state Self::State>;
    /// Exact owner declaration: old Kernel-only adapters cannot consume Data.
    fn has_invocation_data(&self) -> bool {
        false
    }
    /// Exact owner setup may precede row traversal and reject an empty carrier.
    /// Other owners retain the original empty-invocation preparation skip.
    fn requires_empty_update_preparation(&self) -> bool {
        false
    }
    /// Only owners whose emission consumes the real host receipt opt in.
    fn requires_emission_context(&self) -> bool {
        false
    }
    /// Immutable declaration for framework helper backing on the same host.
    /// Existing owners make no new resource-coverage claim by default.
    fn requires_helper_memory(&self) -> bool {
        false
    }
    fn prepare_update_evaluation<'batch>(
        &'batch self,
        input: SelectedAggregateUpdateInput<'batch, 'batch>,
        _mapping: &[usize],
        _allocator: Option<Arc<dyn crate::AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedUpdateBatch<'batch>, crate::EvaluationFailure> {
        self.prepare_update(input, control).map_err(Into::into)
    }
    fn update_row_evaluation<'batch>(
        &self,
        state: &mut Self::State,
        prepared: &Self::PreparedUpdateBatch<'batch>,
        selected_ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), crate::EvaluationFailure> {
        self.update_row(state, prepared, selected_ordinal, control)
            .map_err(Into::into)
    }
    fn prepare_merge_evaluation<'batch>(
        &'batch self,
        input: SelectedAggregateMergeInput<'batch, 'batch>,
        _mapping: &[usize],
        _allocator: Option<Arc<dyn crate::AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedMergeBatch<'batch>, crate::EvaluationFailure> {
        self.prepare_merge(input, control).map_err(Into::into)
    }
    fn merge_row_evaluation<'batch>(
        &self,
        state: &mut Self::State,
        prepared: &Self::PreparedMergeBatch<'batch>,
        selected_ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), crate::EvaluationFailure> {
        self.merge_row(state, prepared, selected_ordinal, control)
            .map_err(Into::into)
    }
    fn build_intermediate_evaluation<'state, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, crate::EvaluationFailure>
    where
        Self::State: 'state,
        I: ExactSizeIterator<Item = &'state Self::State>,
    {
        self.build_intermediate(states, control).map_err(Into::into)
    }
    fn build_final_evaluation<'state, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, crate::EvaluationFailure>
    where
        Self::State: 'state,
        I: ExactSizeIterator<Item = &'state Self::State>,
    {
        self.build_final(states, control).map_err(Into::into)
    }
    /// Default behavior preserves the original builder and checkpoint sequence.
    fn build_intermediate_evaluation_with_context<'state, I>(
        &self,
        states: I,
        _context: &AggregateEmissionContext<'_>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, crate::EvaluationFailure>
    where
        Self::State: 'state,
        I: ExactSizeIterator<Item = &'state Self::State> + Clone,
    {
        self.build_intermediate_evaluation(states, control)
    }
    fn build_final_evaluation_with_context<'state, I>(
        &self,
        states: I,
        _context: &AggregateEmissionContext<'_>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, crate::EvaluationFailure>
    where
        Self::State: 'state,
        I: ExactSizeIterator<Item = &'state Self::State> + Clone,
    {
        self.build_final_evaluation(states, control)
    }
    fn prepared_update_retained_bytes(&self, _prepared: &Self::PreparedUpdateBatch<'_>) -> usize {
        0
    }
    fn prepared_merge_retained_bytes(&self, _prepared: &Self::PreparedMergeBatch<'_>) -> usize {
        0
    }
    /// O(1) additional owned heap, including retained growth on error exits.
    /// The host accounts inline State in its arena separately.
    fn retained_bytes(&self, state: &Self::State) -> usize;
}

/// The exact pure owner returns a concrete immutable kernel. No live state or
/// service can be obtained during prepare. Process composition keeps the typed
/// adapter with this resolved object; runtime must not resolve by name again.
pub trait PureAggregateImplementation:
    FunctionBindingResolver + FunctionEffectOwner<Error = FunctionBindingError>
{
    type Kernel: PreparedAggregateKernel;
    fn prepare_aggregate(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<AggregateCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<Self::Kernel>, KernelFailure>;
}

#[derive(Clone, Debug)]
pub struct AggregatePreparationOptions {
    pub state_interpretation: Option<Arc<novarocks_type_contract::AggregateStateInterpretation>>,
    pub phase: AggregateKernelPhase,
    pub distinct: bool,
    pub order_keys: Arc<[AggregateOrderKey]>,
    pub state_input_type: Option<FunctionValueType>,
}
#[derive(Debug)]
pub struct AggregateSpecialization<K: PreparedAggregateKernel> {
    prepared: Arc<K>,
    effects: ScopedExpressionEffects,
}
impl<K: PreparedAggregateKernel> AggregateSpecialization<K> {
    pub fn prepared(&self) -> &Arc<K> {
        &self.prepared
    }
    pub const fn effects(&self) -> ScopedExpressionEffects {
        self.effects
    }
    pub fn into_prepared(self) -> Arc<K> {
        self.prepared
    }
    pub fn state_layout(&self) -> Layout {
        Layout::new::<K::State>()
    }
}

pub fn specialize_aggregate<O: PureAggregateImplementation + ?Sized>(
    owner: &O,
    input: CallEffectInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    arguments: ScopedExpressionEffects,
    options: AggregatePreparationOptions,
    control: &dyn PureCompileControl,
) -> Result<AggregateSpecialization<O::Kernel>, FunctionSpecializationFailure> {
    specialize_aggregate_once(
        (owner, O::prepare_aggregate),
        input,
        selected,
        None,
        arguments,
        options,
        control,
    )
}
pub fn specialize_frozen_aggregate<O: PureAggregateImplementation + ?Sized>(
    owner: &O,
    input: CallEffectInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    frozen: &CallEffects,
    arguments: ScopedExpressionEffects,
    options: AggregatePreparationOptions,
    control: &dyn PureCompileControl,
) -> Result<AggregateSpecialization<O::Kernel>, FunctionSpecializationFailure> {
    specialize_aggregate_once(
        (owner, O::prepare_aggregate),
        input,
        selected,
        Some(frozen),
        arguments,
        options,
        control,
    )
}
pub(crate) type AggregatePreparationMethod<O> =
    fn(
        &O,
        CallEffectInput<'_>,
        Arc<AggregateCallContract>,
        &dyn PureCompileControl,
    ) -> Result<Arc<<O as PureAggregateImplementation>::Kernel>, KernelFailure>;

/// Both use points retain one refinement, contract and preparation author.
pub(crate) fn specialize_aggregate_once<O: PureAggregateImplementation + ?Sized>(
    preparation: (&O, AggregatePreparationMethod<O>),
    input: CallEffectInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    frozen: Option<&CallEffects>,
    arguments: ScopedExpressionEffects,
    options: AggregatePreparationOptions,
    control: &dyn PureCompileControl,
) -> Result<AggregateSpecialization<O::Kernel>, FunctionSpecializationFailure> {
    let (owner, prepare) = preparation;
    let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
        control,
        CompilePhase::FunctionSpecialization,
    )
    .map_err(FunctionSpecializationFailure::Control)?;
    let aligned = (|| {
        let phase = options.phase;
        let correct = match input.argument_uses {
            crate::CallArgumentUses::TemporalSources { .. }
            | crate::CallArgumentUses::RegexpCountPattern { .. }
            | crate::CallArgumentUses::ToBase64Bytes { .. } => false,
            crate::CallArgumentUses::SelectedChannels(_) => {
                phase.consumes_logical_arguments() && options.state_input_type.is_none()
            }
            crate::CallArgumentUses::AggregateMerge { phase: actual, .. } => {
                !phase.consumes_logical_arguments()
                    && actual == phase
                    && options.state_input_type.is_some()
            }
        };
        work.step()
            .map_err(crate::kernel_control::compile_failure)?;
        if !correct {
            return Err(invalid(
                "aggregate runtime demand differs from its exact preparation phase",
            ));
        }
        match input.argument_uses {
            crate::CallArgumentUses::TemporalSources { .. }
            | crate::CallArgumentUses::RegexpCountPattern { .. }
            | crate::CallArgumentUses::ToBase64Bytes { .. } => Err(invalid(
                "aggregate preparation rejects scalar temporal source channels",
            )),
            crate::CallArgumentUses::SelectedChannels(_) => Ok(None),
            crate::CallArgumentUses::AggregateMerge {
                state_input_type, ..
            } => {
                let Some(owned) = options.state_input_type else {
                    return Err(invalid("aggregate merge options have no state input type"));
                };
                crate::aggregate_call::align_aggregate_merge_state_observed(
                    state_input_type,
                    owned,
                    &mut work,
                )
                .map(Some)
            }
        }
    })();
    let aligned = match aligned {
        Err(
            error @ (KernelFailure::Cancelled
            | KernelFailure::DeadlineExceeded
            | KernelFailure::ResourceExhausted),
        ) => {
            return Err(match error {
                KernelFailure::Cancelled => FunctionSpecializationFailure::Control(
                    novarocks_type_contract::CompileControlError::Cancelled,
                ),
                KernelFailure::DeadlineExceeded => FunctionSpecializationFailure::Control(
                    novarocks_type_contract::CompileControlError::DeadlineExceeded,
                ),
                KernelFailure::ResourceExhausted => FunctionSpecializationFailure::Control(
                    novarocks_type_contract::CompileControlError::ResourceExhausted,
                ),
                other => FunctionSpecializationFailure::Kernel(other),
            });
        }
        result => {
            work.finish()
                .map_err(FunctionSpecializationFailure::Control)?;
            result.map_err(FunctionSpecializationFailure::Kernel)?
        }
    };
    let (receipt, effects) = crate::specialization::refine_once_for_specialization(
        owner, input, frozen, arguments, control,
    )?;
    let call = Arc::new(
        FunctionCallContract::from_refined(input, &receipt, selected, control)
            .map_err(FunctionSpecializationFailure::Kernel)?,
    );
    let contract = Arc::new(
        AggregateCallContract::try_new_refined(
            call,
            options.phase,
            options.distinct,
            options.order_keys,
            options.state_interpretation,
            aligned,
            receipt.aggregate_merge_state(),
            control,
        )
        .map_err(FunctionSpecializationFailure::Kernel)?,
    );
    let prepared = prepare(owner, input, contract.clone(), control)
        .map_err(FunctionSpecializationFailure::Kernel)?;
    if !Arc::ptr_eq(prepared.contract(), &contract) {
        return Err(FunctionSpecializationFailure::Kernel(internal(
            "aggregate preparation replaced its exact immutable contract",
        )));
    }
    state_retained_bound(prepared.as_ref()).map_err(FunctionSpecializationFailure::Kernel)?;
    control
        .checkpoint(CompilePhase::FunctionSpecialization, 0)
        .map_err(FunctionSpecializationFailure::Control)?;
    Ok(AggregateSpecialization { prepared, effects })
}

pub(crate) fn state_retained_bound<K: PreparedAggregateKernel>(
    kernel: &K,
) -> Result<Option<usize>, KernelFailure> {
    let heap = match kernel.memory_policy() {
        AggregateStateMemoryPolicy::AllocationTracked => return Ok(None),
        AggregateStateMemoryPolicy::FixedZero => 0,
        AggregateStateMemoryPolicy::BoundedRetained {
            max_retained_bytes_per_state,
        } => max_retained_bytes_per_state,
    };
    size_of::<K::State>()
        .checked_add(heap)
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok(Some(heap))
}
fn validate_state_retained<K: PreparedAggregateKernel>(
    kernel: &K,
    state: &K::State,
) -> Result<(), KernelFailure> {
    if state_retained_bound(kernel)?.is_some_and(|bound| kernel.retained_bytes(state) > bound) {
        Err(internal(
            "aggregate state exceeded its immutable retained bound",
        ))
    } else {
        Ok(())
    }
}
/// Returns an unpublished typed state only after successful initialization.
/// The host publishes it into its arena afterwards and owns exact Drop, even
/// during cancellation. State-owned partial construction cleanup remains Rust
/// RAII. Global-empty versus grouped-empty state creation is a host decision.
pub fn create_aggregate_state<K: PreparedAggregateKernel>(
    kernel: &K,
    control: &dyn KernelEvaluationControl,
) -> Result<K::State, KernelFailure> {
    create_aggregate_state_with_allocator(kernel, None, control)
}

pub fn create_aggregate_state_with_allocator<K: PreparedAggregateKernel>(
    kernel: &K,
    allocator: Option<Arc<dyn crate::AggregateStateAllocator>>,
    control: &dyn KernelEvaluationControl,
) -> Result<K::State, KernelFailure> {
    control.checkpoint(0)?;
    state_retained_bound(kernel)?;
    if kernel.memory_policy() == AggregateStateMemoryPolicy::AllocationTracked
        && allocator.is_none()
    {
        return Err(invalid(
            "allocation-tracked aggregate state requires a host allocator",
        ));
    }
    let state = kernel.create_state_with_allocator(allocator, control)?;
    validate_state_retained(kernel, &state)?;
    control.checkpoint(0)?;
    Ok(state)
}

/// One borrowed selected batch; success advances once, failure latches. The
/// host maps next_selected_ordinal to one short exclusive group-state borrow.
/// Repeated group mappings are legal; no aliased mutable state slice exists.
/// The host must also latch the whole operator after failure: rebuilding this
/// batch cannot authorize replay of an already applied prefix. Concrete State
/// type identity is insufficient: the host retains exact specialization and
/// initialization/Drop identity for every supplied state.
pub struct AggregateUpdateInvocation<'batch, K: PreparedAggregateKernel> {
    kernel: &'batch K,
    input: SelectedAggregateUpdateInput<'batch, 'batch>,
    prepared: Option<K::PreparedUpdateBatch<'batch>>,
    next: usize,
    failed: bool,
}
impl<'batch, K: PreparedAggregateKernel> AggregateUpdateInvocation<'batch, K> {
    pub fn try_new(
        kernel: &'batch K,
        input: SelectedAggregateUpdateInput<'batch, 'batch>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, KernelFailure> {
        if kernel.has_invocation_data() {
            return Err(invalid("aggregate requires its invocation Data protocol"));
        }
        kernel_only_result(Self::try_new_evaluation(kernel, input, &[], None, control))
    }
    pub fn try_new_evaluation(
        kernel: &'batch K,
        input: SelectedAggregateUpdateInput<'batch, 'batch>,
        mapping: &[usize],
        allocator: Option<Arc<dyn crate::AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, crate::EvaluationFailure> {
        control.checkpoint(0)?;
        if !std::ptr::eq(input.contract(), kernel.contract().as_ref()) {
            return Err(
                invalid("aggregate update input differs from exact prepared contract").into(),
            );
        }
        let prepared =
            if input.selection().is_empty() && !kernel.requires_empty_update_preparation() {
                None
            } else {
                Some(kernel.prepare_update_evaluation(input, mapping, allocator, control)?)
            };
        control.checkpoint(0)?;
        Ok(Self {
            kernel,
            input,
            prepared,
            next: 0,
            failed: false,
        })
    }
    pub fn retained_preparation_bytes(&self) -> usize {
        self.prepared.as_ref().map_or(0, |prepared| {
            self.kernel.prepared_update_retained_bytes(prepared)
        })
    }
    pub fn next_selected_ordinal(&self) -> Option<usize> {
        (!self.failed && self.next < self.input.selection().len()).then_some(self.next)
    }
    pub fn next_batch_row(&self) -> Option<usize> {
        self.next_selected_ordinal()
            .and_then(|ordinal| self.input.selection().row(ordinal))
    }
    pub fn update_next(
        &mut self,
        state: &mut K::State,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        if self.kernel.has_invocation_data() {
            return Err(invalid("aggregate requires its invocation Data protocol"));
        }
        kernel_only_result(self.update_next_evaluation(state, control))
    }
    pub fn update_next_evaluation(
        &mut self,
        state: &mut K::State,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), crate::EvaluationFailure> {
        if self.failed {
            return Err(KernelFailure::InstanceFailed.into());
        }
        let Some(ordinal) = self.next_selected_ordinal() else {
            return Err(invalid("aggregate update has no remaining selected row").into());
        };
        let result = (|| {
            control.checkpoint(0)?;
            validate_state_retained(self.kernel, state)?;
            let result = self.kernel.update_row_evaluation(
                state,
                self.prepared.as_ref().expect("nonempty checked invocation"),
                ordinal,
                control,
            );
            // Observe retained growth even on error exits. Control/resources
            // remain the primary failure; no post-check grants allocation.
            crate::evaluation_failure::finish_evaluation_lifecycle(result, || {
                validate_state_retained(self.kernel, state)
            })
        })();
        match result {
            Ok(()) => {
                self.next += 1;
                Ok(())
            }
            Err(error) => {
                self.failed = true;
                Err(error)
            }
        }
    }
}

pub struct AggregateMergeInvocation<'batch, K: PreparedAggregateKernel> {
    kernel: &'batch K,
    input: SelectedAggregateMergeInput<'batch, 'batch>,
    prepared: Option<K::PreparedMergeBatch<'batch>>,
    next: usize,
    failed: bool,
}
impl<'batch, K: PreparedAggregateKernel> AggregateMergeInvocation<'batch, K> {
    pub fn try_new(
        kernel: &'batch K,
        input: SelectedAggregateMergeInput<'batch, 'batch>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, KernelFailure> {
        if kernel.has_invocation_data() {
            return Err(invalid("aggregate requires its invocation Data protocol"));
        }
        kernel_only_result(Self::try_new_evaluation(kernel, input, &[], None, control))
    }
    pub fn try_new_evaluation(
        kernel: &'batch K,
        input: SelectedAggregateMergeInput<'batch, 'batch>,
        mapping: &[usize],
        allocator: Option<Arc<dyn crate::AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, crate::EvaluationFailure> {
        control.checkpoint(0)?;
        if !std::ptr::eq(input.contract(), kernel.contract().as_ref()) {
            return Err(
                invalid("aggregate merge input differs from exact prepared contract").into(),
            );
        }
        let prepared = if input.selection().is_empty() {
            None
        } else {
            Some(kernel.prepare_merge_evaluation(input, mapping, allocator, control)?)
        };
        control.checkpoint(0)?;
        Ok(Self {
            kernel,
            input,
            prepared,
            next: 0,
            failed: false,
        })
    }
    pub fn retained_preparation_bytes(&self) -> usize {
        self.prepared.as_ref().map_or(0, |prepared| {
            self.kernel.prepared_merge_retained_bytes(prepared)
        })
    }
    pub fn next_selected_ordinal(&self) -> Option<usize> {
        (!self.failed && self.next < self.input.selection().len()).then_some(self.next)
    }
    pub fn next_batch_row(&self) -> Option<usize> {
        self.next_selected_ordinal()
            .and_then(|ordinal| self.input.selection().row(ordinal))
    }
    pub fn merge_next(
        &mut self,
        state: &mut K::State,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        if self.kernel.has_invocation_data() {
            return Err(invalid("aggregate requires its invocation Data protocol"));
        }
        kernel_only_result(self.merge_next_evaluation(state, control))
    }
    pub fn merge_next_evaluation(
        &mut self,
        state: &mut K::State,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), crate::EvaluationFailure> {
        if self.failed {
            return Err(KernelFailure::InstanceFailed.into());
        }
        let Some(ordinal) = self.next_selected_ordinal() else {
            return Err(invalid("aggregate merge has no remaining selected row").into());
        };
        let result = (|| {
            control.checkpoint(0)?;
            validate_state_retained(self.kernel, state)?;
            let result = self.kernel.merge_row_evaluation(
                state,
                self.prepared.as_ref().expect("nonempty checked invocation"),
                ordinal,
                control,
            );
            crate::evaluation_failure::finish_evaluation_lifecycle(result, || {
                validate_state_retained(self.kernel, state)
            })
        })();
        match result {
            Ok(()) => {
                self.next += 1;
                Ok(())
            }
            Err(error) => {
                self.failed = true;
                Err(error)
            }
        }
    }
}

/// Only a declaration-proven Kernel-only caller uses this projection. A Data
/// result here violates the owner's declaration; real Data owners cannot enter.
pub(crate) fn kernel_only_result<T>(
    result: Result<T, crate::EvaluationFailure>,
) -> Result<T, KernelFailure> {
    match result {
        Ok(value) => Ok(value),
        Err(crate::EvaluationFailure::Kernel(cause)) => Err(cause),
        Err(crate::EvaluationFailure::InvocationData(_)) => Err(internal(
            "aggregate contradicted its Kernel-only failure declaration",
        )),
    }
}

pub(crate) fn finish_lifecycle<T>(
    result: Result<T, KernelFailure>,
    post: Result<(), KernelFailure>,
) -> Result<T, KernelFailure> {
    match result {
        Ok(value) => post.map(|()| value),
        Err(
            primary @ (KernelFailure::Cancelled
            | KernelFailure::DeadlineExceeded
            | KernelFailure::ResourceExhausted),
        ) => Err(primary),
        Err(primary) => match post {
            Err(violation @ KernelFailure::Internal(_)) => Err(violation),
            _ => Err(primary),
        },
    }
}

fn validate_emission_states<'state, K, I>(
    kernel: &K,
    states: I,
    control: &dyn KernelEvaluationControl,
) -> Result<(), KernelFailure>
where
    K: PreparedAggregateKernel,
    K::State: 'state,
    I: ExactSizeIterator<Item = &'state K::State>,
{
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    for state in states {
        validate_state_retained(kernel, state)?;
        work.step()?;
    }
    work.finish()
}

/// Emit complete supplied group states according to the exact call phase.
/// Iterator Clone must be a bounded borrowed view, never a state/schema copy.
/// The host preserves exact initialized state/kernel identities; sharing a
/// concrete State type does not prove the same specialization or provenance.
/// Row capacity is a host output-shape grant, not peak-byte authorization.
/// No outer scalar Selection can suppress required aggregate setup or errors.
/// Post-checking can itself stop on control; in that case it has not validated
/// every state. The host still reconciles and destroys actual retained backing.
pub fn emit_aggregate<'state, K, I>(
    kernel: &K,
    states: I,
    row_capacity: usize,
    control: &dyn KernelEvaluationControl,
) -> Result<ArrayRef, KernelFailure>
where
    K: PreparedAggregateKernel,
    K::State: 'state,
    I: ExactSizeIterator<Item = &'state K::State> + Clone,
{
    if kernel.has_invocation_data() {
        return Err(invalid("aggregate requires its invocation Data protocol"));
    }
    kernel_only_result(emit_aggregate_evaluation(
        kernel,
        states,
        row_capacity,
        control,
    ))
}

pub fn emit_aggregate_evaluation<'state, K, I>(
    kernel: &K,
    states: I,
    row_capacity: usize,
    control: &dyn KernelEvaluationControl,
) -> Result<ArrayRef, crate::EvaluationFailure>
where
    K: PreparedAggregateKernel,
    K::State: 'state,
    I: ExactSizeIterator<Item = &'state K::State> + Clone,
{
    emit_aggregate_evaluation_in(kernel, states, row_capacity, None, control)
}

pub(crate) fn emit_aggregate_evaluation_in<'state, K, I>(
    kernel: &K,
    states: I,
    row_capacity: usize,
    context: Option<&AggregateEmissionContext<'_>>,
    control: &dyn KernelEvaluationControl,
) -> Result<ArrayRef, crate::EvaluationFailure>
where
    K: PreparedAggregateKernel,
    K::State: 'state,
    I: ExactSizeIterator<Item = &'state K::State> + Clone,
{
    // Declare the lease first: the observer and its private Mutex backing
    // must be destroyed before the temporary reservation is released.
    let _helper_lease = if kernel.requires_helper_memory() {
        Some(crate::aggregate_helper_memory::reserve_observation_backing(
            context.and_then(AggregateEmissionContext::allocator),
        )?)
    } else {
        None
    };
    let observed = crate::kernel_control::KernelControlObservation::new(control);
    if _helper_lease.is_some() {
        // Exclusively initialize the fresh journal without invoking control.
        observed.finish(Ok(()))?;
    }
    let result =
        emit_aggregate_evaluation_observed(kernel, states, row_capacity, context, &observed);
    match result {
        Err(data @ crate::EvaluationFailure::InvocationData(_)) => Err(data),
        Ok(value) => observed.finish(Ok(value)).map_err(Into::into),
        // The lossless operation already returned its originating failure.
        // Its error exit has no optional observation or lifecycle footer.
        Err(crate::EvaluationFailure::Kernel(cause)) if kernel.has_invocation_data() => {
            // Query the already-borrowed journal only: output validation may
            // translate InvalidProgram structural failures, but a recorded
            // control refusal retains its originating class and full cause.
            // finish performs no callback, state traversal or optional footer.
            observed.finish(Err(cause)).map_err(Into::into)
        }
        Err(crate::EvaluationFailure::Kernel(cause)) => {
            observed.finish(Err(cause)).map_err(Into::into)
        }
    }
}

fn emit_aggregate_evaluation_observed<'state, K, I>(
    kernel: &K,
    states: I,
    row_capacity: usize,
    context: Option<&AggregateEmissionContext<'_>>,
    control: &dyn KernelEvaluationControl,
) -> Result<ArrayRef, crate::EvaluationFailure>
where
    K: PreparedAggregateKernel,
    K::State: 'state,
    I: ExactSizeIterator<Item = &'state K::State> + Clone,
{
    control.checkpoint(0)?;
    let rows = states.len();
    if rows > row_capacity {
        return Err(KernelFailure::ResourceExhausted.into());
    }
    if kernel.requires_emission_context() && context.is_none() {
        return Err(invalid("aggregate emission requires its actual host context").into());
    }
    validate_emission_states(kernel, states.clone(), control)?;
    let final_result = kernel.contract().phase().produces_final_result();
    let result = match (final_result, context) {
        (true, Some(context)) => {
            kernel.build_final_evaluation_with_context(states.clone(), context, control)
        }
        (false, Some(context)) => {
            kernel.build_intermediate_evaluation_with_context(states.clone(), context, control)
        }
        (true, None) => kernel.build_final_evaluation(states.clone(), control),
        (false, None) => kernel.build_intermediate_evaluation(states.clone(), control),
    };
    // Lossless owners publish an atomic originating Data or Kernel failure.
    // In particular a host refusal need not have passed through control.
    if kernel.has_invocation_data() && result.is_err() {
        return result;
    }
    // Interrupted output is terminal. The host still owns reconciliation and
    // destruction; another observed state traversal cannot complete this call.
    if matches!(
        &result,
        Err(crate::EvaluationFailure::InvocationData(_)
            | crate::EvaluationFailure::Kernel(
                KernelFailure::Cancelled
                    | KernelFailure::DeadlineExceeded
                    | KernelFailure::ResourceExhausted
            ))
    ) {
        return result;
    }
    // &State may contain interior mutable storage. Both successful and failed
    // emissions are checked, without changing a primary control/resource error.
    let output = crate::evaluation_failure::finish_evaluation_lifecycle(result, || {
        validate_emission_states(kernel, states, control)
    })?;
    let value_type = if kernel.contract().phase().produces_final_result() {
        kernel.contract().final_type()
    } else {
        kernel.contract().intermediate_type()
    };
    validate_argument_observed(
        EvaluatedArgument::Column(&output),
        Selection::all(rows),
        value_type,
        control,
    )
    .map_err(|error| match error {
        KernelFailure::InvalidProgram(_) => {
            internal("aggregate emission differs from exact result type, rows or NULL contract")
        }
        other => other,
    })?;
    Ok(output)
}

#[cfg(test)]
mod tests;
