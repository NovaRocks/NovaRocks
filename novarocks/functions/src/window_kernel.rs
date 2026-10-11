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

//! Pure window preparation and complete-input partition lifecycles.

use crate::aggregate_kernel::{finish_lifecycle, specialize_aggregate_once};
use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::{
    AggregateCallContract, AggregatePreparationOptions, CallEffectInput, FullPartitionWindowInput,
    FunctionBindingError, FunctionBindingResolver, FunctionBindingSelection, FunctionCallContract,
    FunctionEffectOwner, FunctionSpecializationFailure, KernelEvaluationControl, KernelFailure,
    PreparedAggregateKernel, PureAggregateImplementation, ScopedExpressionEffects, SelectedValues,
    Selection, WindowCallContract, WindowCallOptions, WindowOutputProjection,
};
use novarocks_type_contract::{
    CallEffects, CompilePhase, PureCompileControl, WindowFrameExclusion,
};
use std::{fmt, sync::Arc};

/// Half-open local partition row coordinates. Empty frames are legal; peer
/// groups are nonempty. Bounds do not prove sorting or SQL frame derivation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WindowRowRange {
    pub start: usize,
    pub end: usize,
}

#[cfg(test)]
mod aggregate_tests;
#[cfg(test)]
mod tests;

/// Complete partition geometry supplied by the operator's ORDER/frame owner.
/// This view makes no independent comparison or default-frame decision.
/// Contiguous frames express NO OTHERS only. Other exclusions require an
/// actual membership implementation, rather than silently ignoring options.
#[derive(Clone, Copy, Debug)]
pub struct WindowPartitionInput<'input> {
    input: FullPartitionWindowInput<'input, 'input>,
    peers: &'input [WindowRowRange],
    frames: &'input [WindowRowRange],
}
impl<'input> WindowPartitionInput<'input> {
    pub fn try_new(
        input: FullPartitionWindowInput<'input, 'input>,
        peers: &'input [WindowRowRange],
        frames: &'input [WindowRowRange],
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, KernelFailure> {
        control.checkpoint(0)?;
        if input
            .contract()
            .options()
            .frame()
            .is_some_and(|frame| frame.exclusion != WindowFrameExclusion::NoOthers)
        {
            return Err(invalid("contiguous window frames require NO OTHERS"));
        }
        let rows = input.partition_rows();
        if frames.len() != rows {
            return Err(invalid(
                "window frame table differs from complete partition",
            ));
        }
        let mut work = EvaluationCheckpoints::new(control);
        let mut end = 0;
        for peer in peers {
            if peer.start != end || peer.start >= peer.end || peer.end > rows {
                return Err(invalid(
                    "window peers do not continuously cover the partition",
                ));
            }
            end = peer.end;
            work.step()?;
        }
        if end != rows {
            return Err(invalid("window peers do not cover the complete partition"));
        }
        for frame in frames {
            if frame.start > frame.end || frame.end > rows {
                return Err(invalid("window frame is outside the complete partition"));
            }
            work.step()?;
        }
        work.finish()?;
        Ok(Self {
            input,
            peers,
            frames,
        })
    }
    pub const fn full_input(self) -> FullPartitionWindowInput<'input, 'input> {
        self.input
    }
    pub const fn peers(self) -> &'input [WindowRowRange] {
        self.peers
    }
    pub const fn frames(self) -> &'input [WindowRowRange] {
        self.frames
    }
}

/// Immutable resolved CPU preparation; no state, ExprArena, Task, connector,
/// lookup service or capacity account is stored here. The exact specializer
/// checks frame/NULL capabilities and required constants before publication.
/// Aggregate OVER uses an exact Single aggregate owner plus a window adapter,
/// retaining its aggregate identity and separate function ORDER channels.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WindowPartitionRetention {
    RowBounded {
        bytes: usize,
    },
    /// Complete input-backed carrier frozen after required setup. This only
    /// chooses the retention validator. Actual host grants precede every
    /// output operation; the observed carrier fact grants no allocation.
    InputBackedFrozen,
}

pub trait PreparedWindowKernel: Send + Sync + fmt::Debug + 'static {
    /// Original analytic native family fact. Existing nonaggregate window
    /// owners do not authorize partition-only regrouping. Aggregate wrappers
    /// must author this from their actual original family, never from names.
    fn original_partition_regroup_eligible(&self) -> bool {
        false
    }

    fn contract(&self) -> &Arc<WindowCallContract>;
    /// O(1) complete bound of one boxed partition body and owned heap for the
    /// given row count, including bounded growth on error exits. Borrowed
    /// columns/geometry remain under their host owners. This is no MEM grant.
    fn partition_retained_upper_bound(&self, rows: usize) -> Result<usize, KernelFailure>;
    fn has_complete_carrier_handoff(&self) -> bool {
        false
    }
    /// Positive owner proof for original function-major whole-input assembly.
    /// Existing row-bounded owners keep their established partition route.
    fn has_original_invocation_handoff(&self) -> bool {
        false
    }

    fn partition_retention(&self, rows: usize) -> Result<WindowPartitionRetention, KernelFailure> {
        self.partition_retained_upper_bound(rows)
            .map(|bytes| WindowPartitionRetention::RowBounded { bytes })
    }
    /// Explicit whole-invocation source. This does not reinterpret a partition
    /// loan, infer an invocation mode, or supply a missing host capability.
    fn begin_invocation_evaluated<'input>(
        self: Arc<Self>,
        _input: crate::WindowInvocationInput<'input>,
        _context: crate::WindowInvocationContext,
        _allocator: Option<Arc<dyn crate::AggregateStateAllocator>>,
        _control: &dyn KernelEvaluationControl,
    ) -> Result<Box<dyn WindowKernelPartition + 'input>, crate::WindowEvaluationFailure> {
        Err(invalid("prepared window owner has no whole-invocation source capability").into())
    }
    fn begin_partition_evaluated<'input>(
        self: Arc<Self>,
        input: WindowPartitionInput<'input>,
        _context: crate::window_invocation_data::WindowInvocationContext,
        _allocator: Option<Arc<dyn crate::AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<
        Box<dyn WindowKernelPartition + 'input>,
        crate::window_invocation_data::WindowEvaluationFailure,
    > {
        self.begin_partition(input, control).map_err(Into::into)
    }

    /// Called with complete input even when all output is guarded away. The
    /// owner must complete all required partition setup/evaluation and expose
    /// its required failures here, independently of output demand. Passing
    /// complete columns alone is insufficient: an Aggregate OVER adapter may
    /// not postpone required frame updates/overflow until selected emission.
    /// Arc receiver lets typed instances retain their exact immutable owner
    /// without a self-referential runtime object or raw lifetime conversion.
    fn begin_partition<'input>(
        self: Arc<Self>,
        input: WindowPartitionInput<'input>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Box<dyn WindowKernelPartition + 'input>, KernelFailure>;
}

/// A mutable, input-borrowed CPU partition. Dispatch is once per selected
/// output batch. Setup and required input failures have no scalar row masking.
/// Actual construction, mutation, builders and Drop run under host MEM scopes;
/// post-call retained checks cannot authorize prior allocations or peaks.
pub trait WindowKernelPartition: Send {
    /// Output demand only. Required partition work/errors already belong to
    /// begin_partition; implementations cannot move them behind this Selection.
    /// This method may build selected results and report its own outer faults.
    fn evaluate<'selection>(
        &mut self,
        selection: Selection<'selection>,
        row_capacity: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'selection>, KernelFailure>;
    fn finish(&mut self, control: &dyn KernelEvaluationControl) -> Result<(), KernelFailure>;
    fn evaluate_evaluated<'selection>(
        &mut self,
        selection: Selection<'selection>,
        row_capacity: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'selection>, crate::window_invocation_data::WindowEvaluationFailure>
    {
        self.evaluate(selection, row_capacity, control)
            .map_err(Into::into)
    }
    fn finish_evaluated(
        &mut self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), crate::window_invocation_data::WindowEvaluationFailure> {
        self.finish(control).map_err(Into::into)
    }

    fn complete_carrier_evaluated(
        &mut self,
        _control: &dyn KernelEvaluationControl,
    ) -> Result<
        Option<crate::window_result_carrier::WindowResultCarrier>,
        crate::window_invocation_data::WindowEvaluationFailure,
    > {
        Ok(None)
    }
    /// Exact O(1) owned body/heap fact, including failure exits.
    fn retained_bytes(&self) -> usize;
}

pub trait PureWindowImplementation:
    FunctionBindingResolver + FunctionEffectOwner<Error = FunctionBindingError>
{
    fn prepare_window(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<WindowCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedWindowKernel>, KernelFailure>;
}

/// The same exact Aggregate owner provides its dedicated window adapter.
/// The argument is a real immutable typed Single kernel, already refined and
/// prepared once. This entry does not resolve a new Window-kind overload.
pub trait PureAggregateWindowImplementation: PureAggregateImplementation {
    /// Prepare the exact Single aggregate at its window use point. Default
    /// owners retain their original method and control trace without a wrapper.
    fn prepare_aggregate_for_window(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<AggregateCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<Self::Kernel>, KernelFailure> {
        self.prepare_aggregate(input, contract, control)
    }

    fn prepare_aggregate_window(
        &self,
        aggregate: Arc<Self::Kernel>,
        contract: Arc<WindowCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedWindowKernel>, KernelFailure>;
}
#[derive(Debug)]
pub struct WindowSpecialization {
    prepared: Arc<dyn PreparedWindowKernel>,
    effects: ScopedExpressionEffects,
}
impl WindowSpecialization {
    pub fn prepared(&self) -> &Arc<dyn PreparedWindowKernel> {
        &self.prepared
    }
    pub const fn effects(&self) -> ScopedExpressionEffects {
        self.effects
    }
    pub fn into_prepared(self) -> Arc<dyn PreparedWindowKernel> {
        self.prepared
    }
}
pub fn specialize_window<O: PureWindowImplementation + ?Sized>(
    owner: &O,
    input: CallEffectInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    arguments: ScopedExpressionEffects,
    options: WindowCallOptions,
    control: &dyn PureCompileControl,
) -> Result<WindowSpecialization, FunctionSpecializationFailure> {
    specialize_window_once(owner, input, selected, None, arguments, options, control)
}
pub fn specialize_frozen_window<O: PureWindowImplementation + ?Sized>(
    owner: &O,
    input: CallEffectInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    frozen: &CallEffects,
    arguments: ScopedExpressionEffects,
    options: WindowCallOptions,
    control: &dyn PureCompileControl,
) -> Result<WindowSpecialization, FunctionSpecializationFailure> {
    specialize_window_once(
        owner,
        input,
        selected,
        Some(frozen),
        arguments,
        options,
        control,
    )
}
fn specialize_window_once<O: PureWindowImplementation + ?Sized>(
    owner: &O,
    input: CallEffectInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    frozen: Option<&CallEffects>,
    arguments: ScopedExpressionEffects,
    options: WindowCallOptions,
    control: &dyn PureCompileControl,
) -> Result<WindowSpecialization, FunctionSpecializationFailure> {
    let (receipt, effects) = crate::specialization::refine_once_for_specialization(
        owner, input, frozen, arguments, control,
    )?;
    let call = Arc::new(
        FunctionCallContract::from_refined(input, &receipt, selected, control)
            .map_err(FunctionSpecializationFailure::Kernel)?,
    );
    let contract = Arc::new(
        WindowCallContract::try_window(call, options, control)
            .map_err(FunctionSpecializationFailure::Kernel)?,
    );
    let prepared = owner
        .prepare_window(input, contract.clone(), control)
        .map_err(FunctionSpecializationFailure::Kernel)?;
    if !Arc::ptr_eq(prepared.contract(), &contract) {
        return Err(FunctionSpecializationFailure::Kernel(internal(
            "window preparation replaced its exact immutable contract",
        )));
    }
    control
        .checkpoint(CompilePhase::FunctionSpecialization, 0)
        .map_err(FunctionSpecializationFailure::Control)?;
    Ok(WindowSpecialization { prepared, effects })
}

/// Exact Aggregate Single call options and its independent OVER options.
#[derive(Clone, Debug)]
pub struct AggregateWindowPreparationOptions {
    pub aggregate: AggregatePreparationOptions,
    pub window: WindowCallOptions,
}

pub fn specialize_aggregate_window<O: PureAggregateWindowImplementation + ?Sized>(
    owner: &O,
    input: CallEffectInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    arguments: ScopedExpressionEffects,
    options: AggregateWindowPreparationOptions,
    control: &dyn PureCompileControl,
) -> Result<WindowSpecialization, FunctionSpecializationFailure> {
    specialize_aggregate_window_once(owner, input, selected, None, arguments, options, control)
}
pub fn specialize_frozen_aggregate_window<O: PureAggregateWindowImplementation + ?Sized>(
    owner: &O,
    input: CallEffectInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    frozen: &CallEffects,
    arguments: ScopedExpressionEffects,
    options: AggregateWindowPreparationOptions,
    control: &dyn PureCompileControl,
) -> Result<WindowSpecialization, FunctionSpecializationFailure> {
    specialize_aggregate_window_once(
        owner,
        input,
        selected,
        Some(frozen),
        arguments,
        options,
        control,
    )
}
fn specialize_aggregate_window_once<O: PureAggregateWindowImplementation + ?Sized>(
    owner: &O,
    input: CallEffectInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    frozen: Option<&CallEffects>,
    arguments: ScopedExpressionEffects,
    options: AggregateWindowPreparationOptions,
    control: &dyn PureCompileControl,
) -> Result<WindowSpecialization, FunctionSpecializationFailure> {
    let AggregateWindowPreparationOptions {
        aggregate: aggregate_options,
        window: window_options,
    } = options;
    // Fail before even refining/preparing an inappropriate aggregate phase.
    control
        .checkpoint(CompilePhase::FunctionSpecialization, 0)
        .map_err(FunctionSpecializationFailure::Control)?;
    if aggregate_options.phase != crate::AggregateKernelPhase::Single
        || aggregate_options.state_input_type.is_some()
        || !matches!(
            input.argument_uses,
            crate::CallArgumentUses::SelectedChannels(_)
        )
    {
        return Err(FunctionSpecializationFailure::Kernel(invalid(
            "aggregate window specialization requires Single logical input",
        )));
    }
    let specialization = specialize_aggregate_once(
        (owner, O::prepare_aggregate_for_window),
        input,
        selected,
        frozen,
        arguments,
        aggregate_options,
        control,
    )?;
    let effects = specialization.effects();
    let aggregate = specialization.into_prepared();
    let contract = Arc::new(
        WindowCallContract::try_aggregate(aggregate.contract().clone(), window_options, control)
            .map_err(FunctionSpecializationFailure::Kernel)?,
    );
    let prepared = owner
        .prepare_aggregate_window(aggregate, contract.clone(), control)
        .map_err(FunctionSpecializationFailure::Kernel)?;
    if !Arc::ptr_eq(prepared.contract(), &contract) {
        return Err(FunctionSpecializationFailure::Kernel(internal(
            "aggregate window adapter replaced its exact immutable contract",
        )));
    }
    control
        .checkpoint(CompilePhase::FunctionSpecialization, 0)
        .map_err(FunctionSpecializationFailure::Control)?;
    Ok(WindowSpecialization { prepared, effects })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PartitionStatus {
    Open,
    Failed,
    Finished,
}

/// Execution-owned partition instance. Its complete inputs stay borrowed
/// through typed instance destruction; preparation itself stays immutable.
/// An error latches the partition, so a successful prefix cannot be replayed.
pub struct WindowEvaluationPartition<'input> {
    // Rust drops fields in declaration order: typed partition destruction must
    // complete while its exact immutable owner is still alive.
    instance: Box<dyn WindowKernelPartition + 'input>,
    prepared: Arc<dyn PreparedWindowKernel>,
    contract: Arc<WindowCallContract>,
    input: WindowPartitionInput<'input>,
    retained_upper_bound: usize,
    retention: WindowPartitionRetention,
    complete_carrier_handoff: bool,
    first_evaluated_failure: Option<crate::window_invocation_data::WindowEvaluationFailure>,
    status: PartitionStatus,
}
impl<'input> WindowEvaluationPartition<'input> {
    pub fn begin(
        prepared: Arc<dyn PreparedWindowKernel>,
        input: WindowPartitionInput<'input>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, KernelFailure> {
        control.checkpoint(0)?;
        let contract = prepared.contract().clone();
        if !std::ptr::eq(contract.as_ref(), input.full_input().contract()) {
            return Err(invalid(
                "window partition differs from exact prepared contract",
            ));
        }
        let retained_upper_bound =
            prepared.partition_retained_upper_bound(input.full_input().partition_rows())?;
        size_of::<Self>()
            .checked_add(retained_upper_bound)
            .ok_or(KernelFailure::ResourceExhausted)?;
        let instance = prepared.clone().begin_partition(input, control)?;
        let complete_carrier_handoff = prepared.has_complete_carrier_handoff();
        let partition = Self {
            prepared,
            contract,
            input,
            retained_upper_bound,
            retention: WindowPartitionRetention::RowBounded {
                bytes: retained_upper_bound,
            },
            first_evaluated_failure: None,
            complete_carrier_handoff,
            instance,
            status: PartitionStatus::Open,
        };
        partition.validate_retained()?;
        partition.validate_metadata()?;
        control.checkpoint(0)?;
        Ok(partition)
    }
    /// Actual allocator capability and call ordinal come from the invoking
    /// operator; neither is retained in the immutable prepared owner.
    pub fn begin_evaluated(
        prepared: Arc<dyn PreparedWindowKernel>,
        input: WindowPartitionInput<'input>,
        context: crate::window_invocation_data::WindowInvocationContext,
        allocator: Option<Arc<dyn crate::AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, crate::window_invocation_data::WindowEvaluationFailure> {
        if matches!(
            prepared.partition_retention(input.full_input().partition_rows())?,
            WindowPartitionRetention::RowBounded { .. }
        ) {
            return Self::begin(prepared, input, control).map_err(Into::into);
        }
        control.checkpoint(0)?;
        let contract = Arc::clone(prepared.contract());
        if !std::ptr::eq(contract.as_ref(), input.full_input().contract()) {
            return Err(invalid("window partition differs from exact prepared contract").into());
        }
        let retention = prepared.partition_retention(input.full_input().partition_rows())?;
        if let WindowPartitionRetention::RowBounded { bytes } = retention {
            size_of::<Self>()
                .checked_add(bytes)
                .ok_or(KernelFailure::ResourceExhausted)?;
        }
        let instance =
            Arc::clone(&prepared).begin_partition_evaluated(input, context, allocator, control)?;
        let retained_upper_bound = match retention {
            WindowPartitionRetention::RowBounded { bytes } => bytes,
            // This freeze occurs after actual admitted construction; it is a
            // retaining-owner envelope, never authority for the work just done.
            WindowPartitionRetention::InputBackedFrozen => instance.retained_bytes(),
        };
        size_of::<Self>()
            .checked_add(retained_upper_bound)
            .ok_or(KernelFailure::ResourceExhausted)?;
        let complete_carrier_handoff = prepared.has_complete_carrier_handoff();
        let partition = Self {
            instance,
            prepared,
            contract,
            input,
            retained_upper_bound,
            retention,
            complete_carrier_handoff,
            first_evaluated_failure: None,
            status: PartitionStatus::Open,
        };
        partition.validate_retained()?;
        partition.validate_metadata()?;
        control.checkpoint(0)?;
        Ok(partition)
    }
    pub const fn retention_policy(&self) -> WindowPartitionRetention {
        self.retention
    }

    pub fn retained_upper_bound(&self) -> usize {
        size_of::<Self>() + self.retained_upper_bound
    }
    pub fn retained_bytes(&self) -> Result<usize, KernelFailure> {
        self.validate_retained()?;
        Ok(size_of::<Self>() + self.instance.retained_bytes())
    }
    fn validate_retained(&self) -> Result<(), KernelFailure> {
        if self.instance.retained_bytes() > self.retained_upper_bound {
            Err(internal(
                "window partition exceeded its frozen retained bound",
            ))
        } else {
            Ok(())
        }
    }
    fn validate_metadata(&self) -> Result<(), KernelFailure> {
        let immutable_retention = match self.retention {
            WindowPartitionRetention::RowBounded { .. } => {
                self.prepared
                    .partition_retained_upper_bound(self.input.full_input().partition_rows())?
                    == self.retained_upper_bound
            }
            WindowPartitionRetention::InputBackedFrozen => {
                self.prepared
                    .partition_retention(self.input.full_input().partition_rows())?
                    == WindowPartitionRetention::InputBackedFrozen
            }
        };
        if !Arc::ptr_eq(self.prepared.contract(), &self.contract)
            || !immutable_retention
            || self.prepared.has_complete_carrier_handoff() != self.complete_carrier_handoff
        {
            Err(internal(
                "window implementation changed its immutable metadata",
            ))
        } else {
            Ok(())
        }
    }
    pub fn evaluate<'selection>(
        &mut self,
        selection: Selection<'selection>,
        row_capacity: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'selection>, KernelFailure> {
        if self.status != PartitionStatus::Open {
            return Err(KernelFailure::InstanceFailed);
        }
        let result = (|| {
            control.checkpoint(0)?;
            self.validate_metadata()?;
            self.validate_retained()?;
            let input = self.input.full_input();
            let projection = WindowOutputProjection::try_new(&input, selection, control)?;
            if selection.len() > row_capacity {
                return Err(invalid("window output Selection exceeds host row grant"));
            }
            let output = if selection.is_empty() {
                // The complete-input begin already ran. Skipping output work
                // cannot erase required setup/input failures or finish work.
                SelectedValues::try_new(
                    selection,
                    &self.contract.result_type().data_type,
                    arrow_array::new_empty_array(&self.contract.result_type().data_type),
                    Box::default(),
                )
                .map_err(|_| internal("empty window output violates its exact contract"))?
            } else {
                self.instance.evaluate(selection, row_capacity, control)?
            };
            projection.validate_result(&output, row_capacity, control)?;
            control.checkpoint(0)?;
            Ok(output)
        })();
        let result = finish_lifecycle(result, self.validate_retained());
        let result = finish_lifecycle(result, self.validate_metadata());
        if result.is_err() {
            self.status = PartitionStatus::Failed;
        }
        result
    }
    pub fn finish(&mut self, control: &dyn KernelEvaluationControl) -> Result<(), KernelFailure> {
        if self.status != PartitionStatus::Open {
            return Err(KernelFailure::InstanceFailed);
        }
        self.status = PartitionStatus::Finished;
        let result = (|| {
            control.checkpoint(0)?;
            self.validate_metadata()?;
            self.validate_retained()?;
            self.instance.finish(control)?;
            control.checkpoint(0)
        })();
        let result = finish_lifecycle(result, self.validate_retained());
        let result = finish_lifecycle(result, self.validate_metadata());
        if result.is_err() {
            self.status = PartitionStatus::Failed;
        }
        result
    }
    pub fn evaluate_evaluated<'selection>(
        &mut self,
        selection: Selection<'selection>,
        row_capacity: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'selection>, crate::window_invocation_data::WindowEvaluationFailure>
    {
        use crate::window_invocation_data::finish_window_lifecycle;
        if matches!(self.retention, WindowPartitionRetention::RowBounded { .. }) {
            return self
                .evaluate(selection, row_capacity, control)
                .map_err(Into::into);
        }
        if let Some(cause) = &self.first_evaluated_failure {
            return Err(cause.clone());
        }
        if self.status != PartitionStatus::Open {
            return Err(KernelFailure::InstanceFailed.into());
        }
        let result = (|| {
            control.checkpoint(0)?;
            self.validate_metadata()?;
            self.validate_retained()?;
            let input = self.input.full_input();
            let projection = WindowOutputProjection::try_new(&input, selection, control)?;
            if selection.len() > row_capacity {
                return Err(invalid("window output Selection exceeds host row grant").into());
            }
            // Even empty demand reaches the data-aware owner. Required complete
            // setup has already run; no original failure is hidden by Selection.
            let output = self
                .instance
                .evaluate_evaluated(selection, row_capacity, control)?;
            projection.validate_result(&output, row_capacity, control)?;
            control.checkpoint(0)?;
            Ok(output)
        })();
        let result = finish_window_lifecycle(result, || self.validate_retained());
        let result = finish_window_lifecycle(result, || self.validate_metadata());
        if let Err(cause) = &result {
            self.status = PartitionStatus::Failed;
            self.first_evaluated_failure = Some(cause.clone());
        }
        result
    }
    pub fn finish_evaluated(
        &mut self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), crate::window_invocation_data::WindowEvaluationFailure> {
        use crate::window_invocation_data::finish_window_lifecycle;
        if matches!(self.retention, WindowPartitionRetention::RowBounded { .. }) {
            return self.finish(control).map_err(Into::into);
        }
        if let Some(cause) = &self.first_evaluated_failure {
            return Err(cause.clone());
        }
        if self.status != PartitionStatus::Open {
            return Err(KernelFailure::InstanceFailed.into());
        }
        self.status = PartitionStatus::Finished;
        let result = (|| {
            control.checkpoint(0)?;
            self.validate_metadata()?;
            self.validate_retained()?;
            self.instance.finish_evaluated(control)?;
            control.checkpoint(0)?;
            Ok(())
        })();
        let result = finish_window_lifecycle(result, || self.validate_retained());
        let result = finish_window_lifecycle(result, || self.validate_metadata());
        if let Err(cause) = &result {
            self.status = PartitionStatus::Failed;
            self.first_evaluated_failure = Some(cause.clone());
        }
        result
    }
    pub fn complete_carrier_evaluated(
        &mut self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<
        Option<crate::window_result_carrier::WindowResultCarrier>,
        crate::window_invocation_data::WindowEvaluationFailure,
    > {
        use crate::window_invocation_data::finish_window_lifecycle;
        if !self.complete_carrier_handoff {
            return Ok(None);
        }
        if let Some(cause) = &self.first_evaluated_failure {
            return Err(cause.clone());
        }
        if self.status != PartitionStatus::Open {
            return Err(KernelFailure::InstanceFailed.into());
        }
        let result = (|| {
            control.checkpoint(0)?;
            self.validate_metadata()?;
            self.validate_retained()?;
            let carrier = self.instance.complete_carrier_evaluated(control)?;
            let carrier = carrier
                .ok_or_else(|| invalid("complete window handoff owner returned no carrier"))?;
            carrier
                .validate_call_metadata(&self.contract, self.input.full_input().partition_rows())?;
            control.checkpoint(0)?;
            Ok(Some(carrier))
        })();
        let result = finish_window_lifecycle(result, || self.validate_retained());
        let result = finish_window_lifecycle(result, || self.validate_metadata());
        if let Err(cause) = &result {
            self.status = PartitionStatus::Failed;
            self.first_evaluated_failure = Some(cause.clone());
        }
        result
    }
}
