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

//! Allocation-tracked max_by/min_by OVER, using the installed aggregate math.
//! The existing operator supplies frame geometry; this owner never interprets
//! SQL frame syntax or decides partition ordering, peer groups or environment.

use super::aggregate_by::ByKernel;
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_scalar::{self as scalar, ScalarWork};
use crate::kernel_control::{KernelControlObservation, compile_failure, internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::opaque_memory::OpaqueRetainedCharge;
use crate::scalar_output_operation::{RetainedScalarOutput, with_scalar_output_operation};
use crate::scalar_output_resources::ScalarOutputResources;
use crate::window_invocation_data::{
    WindowDataSlot, WindowEvaluationFailure, WindowInvocationContext, WindowInvocationPhase,
};
use crate::window_output_scalars::WindowOutputScalars;
use crate::*;
use arrow_array::Array;
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};
use std::{fmt, sync::Arc};

pub(super) struct TrackedByWindow {
    aggregate: Arc<ByKernel>,
    contract: Arc<WindowCallContract>,
}
impl fmt::Debug for TrackedByWindow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TrackedByWindow")
            .field("aggregate", &self.aggregate)
            .finish_non_exhaustive()
    }
}
pub(super) fn prepare(
    aggregate: Arc<ByKernel>,
    contract: Arc<WindowCallContract>,
    control: &dyn PureCompileControl,
) -> Result<Arc<dyn PreparedWindowKernel>, KernelFailure> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
        .map_err(compile_failure)?;
    let source = aggregate.contract();
    work.step().map_err(compile_failure)?;
    if !contract
        .aggregate()
        .is_some_and(|source| Arc::ptr_eq(source, aggregate.contract()))
        || source.phase() != AggregateKernelPhase::Single
        || source.state_input_type().is_some()
        || source.distinct()
        || !source.order_keys().is_empty()
        || aggregate.memory_policy() != AggregateStateMemoryPolicy::AllocationTracked
    {
        return Err(invalid(
            "tracked BY OVER requires its exact Single aggregate and original channels",
        ));
    }
    work.finish().map_err(compile_failure)?;
    Ok(Arc::new(TrackedByWindow {
        aggregate,
        contract,
    }))
}
fn observed<T>(
    control: &dyn KernelEvaluationControl,
    f: impl FnOnce(
        &mut EvaluationCheckpoints<'_>,
        &dyn KernelEvaluationControl,
    ) -> Result<T, WindowEvaluationFailure>,
) -> Result<T, WindowEvaluationFailure> {
    let observation = KernelControlObservation::new(control);
    observation.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(&observation);
    match f(&mut work, &observation) {
        Err(data @ WindowEvaluationFailure::InvocationData(_)) => Err(data),
        Err(WindowEvaluationFailure::Kernel(cause)) => {
            observation.finish::<T>(Err(cause)).map_err(Into::into)
        }
        Ok(value) => {
            work.finish()?;
            observation.finish(Ok(value)).map_err(Into::into)
        }
    }
}
fn scalar_resource_failure(cause: KernelFailure) -> WindowEvaluationFailure {
    cause.into()
}

// The original update never renders a payload value in its Data message. This
// structural count covers its static reader/comparison diagnostics, type Debug
// and both original stage/ordinal formatters' temporary String growth. It is
// not a per-row UTF8 estimate or a bounded diagnostic truncation policy.
fn update_diagnostic_operation_bytes(
    contract: &WindowCallContract,
    context: WindowInvocationContext,
) -> Result<usize, KernelFailure> {
    struct Count(usize);
    impl fmt::Write for Count {
        fn write_str(&mut self, text: &str) -> fmt::Result {
            self.0 = self.0.checked_add(text.len()).ok_or(fmt::Error)?;
            Ok(())
        }
    }
    use fmt::Write;
    let mut count = Count(0);
    for ty in contract
        .aggregate()
        .expect("checked Single source")
        .call()
        .selected()
        .argument_types
        .iter()
    {
        if let FunctionArgumentType::Value(ty) = ty {
            write!(&mut count, "{:?}", ty).map_err(|_| KernelFailure::ResourceExhausted)?;
        }
    }
    write!(&mut count, "window function #{}: {}",
        context.call_ordinal(), crate::aggregate_format::AggregateFailureStage::Update
            .message("failed to downcast to FixedSizeBinaryArray; tracked scalar comparison type mismatch; float comparison is not ordered"))
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    count
        .0
        .checked_mul(4)
        .ok_or(KernelFailure::ResourceExhausted)
}

// The ONE original frame computation accepts an explicitly checked source
// domain. It never treats several semantic partitions as a partition loan.
#[derive(Clone, Copy)]
enum ByWindowInput<'input> {
    Partition(WindowPartitionInput<'input>),
    Invocation(crate::WindowInvocationInput<'input>),
}
impl<'input> ByWindowInput<'input> {
    fn contract(self) -> &'input WindowCallContract {
        match self {
            Self::Partition(input) => input.full_input().contract(),
            Self::Invocation(input) => input.full_input().contract(),
        }
    }
    fn rows(self) -> usize {
        match self {
            Self::Partition(input) => input.full_input().partition_rows(),
            Self::Invocation(input) => input.full_input().invocation_rows(),
        }
    }
    fn logical_arguments(self) -> &'input [EvaluatedArgument<'input>] {
        match self {
            Self::Partition(input) => input.full_input().logical_arguments(),
            Self::Invocation(input) => input.full_input().logical_arguments(),
        }
    }
    fn frames(self) -> &'input [WindowRowRange] {
        match self {
            Self::Partition(input) => input.frames(),
            Self::Invocation(input) => input.frames(),
        }
    }
    fn frame_origin(
        self,
        row: usize,
        context: WindowInvocationContext,
    ) -> crate::WindowFrameOrigin {
        match self {
            Self::Invocation(input) => input
                .frame_origin(row)
                .expect("checked original frame source"),
            Self::Partition(input) => crate::WindowFrameOrigin {
                partition_ordinal: context
                    .partition_ordinal()
                    .expect("actual partition context"),
                output_row: row,
                partition: WindowRowRange {
                    start: 0,
                    end: input.full_input().partition_rows(),
                },
            },
        }
    }
}
struct ByPartition<'input> {
    // Output buffers must be destroyed before dropping the retaining host
    // handles. External ArrayRef/Buffer loans have their own real custody.
    output: Option<RetainedScalarOutput>,
    owner: Arc<TrackedByWindow>,
    input: ByWindowInput<'input>,
    allocator: HostAggregateAllocator,
    host: Arc<dyn AggregateStateAllocator>,
    data_slot: WindowDataSlot,
    first_failure: Option<WindowEvaluationFailure>,
    finished: bool,
}
impl ByPartition<'_> {
    fn latch<T>(
        &mut self,
        result: Result<T, WindowEvaluationFailure>,
    ) -> Result<T, WindowEvaluationFailure> {
        if let Err(cause) = &result {
            self.first_failure = Some(cause.clone());
            self.output = None;
        }
        result
    }
    fn retained(&self) -> usize {
        size_of::<Self>()
            + self.allocator.metadata_bytes()
            + self.data_slot.retained_envelope()
            + self.output.as_ref().map_or(0, |out| out.retained_envelope)
    }
}
impl PreparedWindowKernel for TrackedByWindow {
    fn original_partition_regroup_eligible(&self) -> bool {
        true
    }
    fn has_complete_carrier_handoff(&self) -> bool {
        true
    }
    fn has_original_invocation_handoff(&self) -> bool {
        true
    }
    fn contract(&self) -> &Arc<WindowCallContract> {
        &self.contract
    }
    fn partition_retained_upper_bound(&self, _rows: usize) -> Result<usize, KernelFailure> {
        Err(invalid(
            "tracked BY OVER requires InputBackedFrozen retention, not a row-only bound",
        ))
    }
    fn partition_retention(&self, _rows: usize) -> Result<WindowPartitionRetention, KernelFailure> {
        Ok(WindowPartitionRetention::InputBackedFrozen)
    }
    fn begin_partition<'input>(
        self: Arc<Self>,
        _input: WindowPartitionInput<'input>,
        _control: &dyn KernelEvaluationControl,
    ) -> Result<Box<dyn WindowKernelPartition + 'input>, KernelFailure> {
        Err(invalid(
            "tracked BY OVER requires the data-aware window invocation and actual runtime allocator",
        ))
    }
    fn begin_partition_evaluated<'input>(
        self: Arc<Self>,
        input: WindowPartitionInput<'input>,
        context: WindowInvocationContext,
        host: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Box<dyn WindowKernelPartition + 'input>, WindowEvaluationFailure> {
        self.initialize_original_frames(ByWindowInput::Partition(input), context, host, control)
    }
    fn begin_invocation_evaluated<'input>(
        self: Arc<Self>,
        input: crate::WindowInvocationInput<'input>,
        context: WindowInvocationContext,
        host: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Box<dyn WindowKernelPartition + 'input>, WindowEvaluationFailure> {
        self.initialize_original_frames(ByWindowInput::Invocation(input), context, host, control)
    }
}
impl TrackedByWindow {
    fn initialize_original_frames<'input>(
        self: Arc<Self>,
        input: ByWindowInput<'input>,
        context: WindowInvocationContext,
        host: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Box<dyn WindowKernelPartition + 'input>, WindowEvaluationFailure> {
        observed(control, |work, control| {
            let source_matches = match input {
                ByWindowInput::Partition(_) => matches!(
                    context.scope(),
                    crate::WindowInvocationScope::Partition { .. }
                ),
                ByWindowInput::Invocation(_) => {
                    context.scope() == crate::WindowInvocationScope::CompleteInvocation
                }
            };
            if !source_matches {
                return Err(
                    invalid("BY source domain differs from explicit invocation context").into(),
                );
            }
            if !std::ptr::eq(input.contract(), self.contract.as_ref()) {
                return Err(invalid(
                    "tracked BY partition differs from its exact checked contract",
                )
                .into());
            }
            let host = host.ok_or_else(|| {
                invalid("tracked BY window requires the actual runtime state allocator")
            })?;
            let diagnostic_charge = OpaqueRetainedCharge::try_new(Arc::clone(&host))?;
            work.flush()?;
            let allocator = HostAggregateAllocator::try_new(Arc::clone(&host))?;
            work.flush()?;
            let rows = input.rows();
            let slot = WindowDataSlot::prepare(
                Arc::clone(&self.contract),
                context,
                rows,
                allocator.clone(),
                work,
            )?;
            let update = SelectedAggregateUpdateInput::try_new(
                self.aggregate.contract(),
                Selection::all(rows),
                input.logical_arguments(),
                &[],
                control,
            )?;
            let update = self.aggregate.prepare_update(update, control)?;
            work.flush()?;
            let diagnostic_bytes = update_diagnostic_operation_bytes(&self.contract, context)?;
            work.flush()?;
            let mut output_values = WindowOutputScalars::try_new(rows, Arc::clone(&host), work)?;
            for (frame_ordinal, frame) in input.frames().iter().enumerate() {
                let frame_origin = input.frame_origin(frame_ordinal, context);
                work.flush()?;
                let mut state = self
                    .aggregate
                    .create_state_with_allocator(Some(Arc::clone(&host)), control)?;
                let result = (|| {
                    for row in frame.start..frame.end {
                        work.flush()?;
                        let reservation = diagnostic_charge.reserve_operation(diagnostic_bytes)?;
                        work.flush()?;
                        let result = observed(control, |row_work, _| {
                            match self.aggregate.update_row_scalar(
                                &mut state,
                                &update,
                                row,
                                &mut ScalarWork::new(Some(row_work)),
                            ) {
                                Ok(()) => Ok(()),
                                // This is already an unmaskable whole computation.
                                // Preserve its source and the actual window invocation
                                // facts under the original pre-granted diagnostic lease.
                                Err(cause)
                                    if {
                                        use novarocks_failure_contract::{
                                            FailureClass, FailureProjection,
                                        };
                                        cause.failure_fact().class() == FailureClass::Data
                                    } =>
                                {
                                    Err(slot
                                        .publish_frame_source(
                                            WindowInvocationPhase::FrameUpdate,
                                            frame_origin,
                                            Some(row),
                                            cause,
                                            reservation,
                                        )
                                        .into())
                                }
                                Err(cause) => Err(cause.into()),
                            }
                        });
                        result?;
                        work.step()?;
                    }
                    let resources = ScalarOutputResources::from_tracked(
                        self.contract.result_type().data_type(),
                        state.value.as_ref(),
                        &mut ScalarWork::new(Some(work)),
                    )
                    .map_err(scalar_resource_failure)?;
                    let output = with_scalar_output_operation(
                        resources,
                        Arc::clone(&host),
                        &allocator,
                        work,
                        |scalar_work| {
                            self.aggregate
                                .build_final_scalar(std::iter::once(&state), scalar_work)
                        },
                    );
                    output.map_err(WindowEvaluationFailure::from)
                })();
                // Same original destruction author: every initialized state is
                // destroyed before inspecting the frame result or reading it.
                drop(state);
                let output = result?;
                output_values
                    .push_original_read(&output.values, 0, output.resources, work)
                    .map_err(WindowEvaluationFailure::from)?;
                work.step()?;
            }
            let resources = ScalarOutputResources::from_owned(
                self.contract.result_type().data_type(),
                output_values.values(),
                &mut ScalarWork::new(Some(work)),
            )
            .map_err(scalar_resource_failure)?;
            let (values, staging_charge) = output_values.into_parts();
            let output = with_scalar_output_operation(
                resources,
                Arc::clone(&host),
                &allocator,
                work,
                |scalar_work| {
                    if rows == 0 {
                        // The original analytic author skips scalar assembly for an
                        // empty output and returns this exact null Arrow carrier.
                        Ok(arrow_array::new_null_array(
                            self.contract.result_type().data_type(),
                            0,
                        ))
                    } else {
                        scalar::build_scalar_array(
                            self.contract.result_type().data_type(),
                            values,
                            scalar_work,
                        )
                    }
                },
            );
            drop(staging_charge);
            let output = output.map_err(WindowEvaluationFailure::from)?;
            work.flush()?;
            Ok(Box::new(ByPartition {
                output: Some(output),
                owner: self,
                input,
                allocator,
                host,
                data_slot: slot,
                first_failure: None,
                finished: false,
            }) as Box<dyn WindowKernelPartition + 'input>)
        })
    }
}
impl WindowKernelPartition for ByPartition<'_> {
    fn evaluate<'selection>(
        &mut self,
        _selection: Selection<'selection>,
        _row_capacity: usize,
        _control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'selection>, KernelFailure> {
        Err(invalid(
            "tracked BY OVER requires the data-aware window output channel",
        ))
    }
    fn finish(&mut self, _control: &dyn KernelEvaluationControl) -> Result<(), KernelFailure> {
        Err(invalid(
            "tracked BY OVER requires the data-aware window finish channel",
        ))
    }
    fn complete_carrier_evaluated(
        &mut self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Option<crate::window_result_carrier::WindowResultCarrier>, WindowEvaluationFailure>
    {
        if let Some(cause) = &self.first_failure {
            return Err(cause.clone());
        }
        if self.finished {
            return Err(KernelFailure::InstanceFailed.into());
        }
        let result = observed(control, |_work, _| {
            let output = self
                .output
                .as_ref()
                .expect("open partition owns its actual original carrier");
            Ok(Some(
                crate::window_result_carrier::WindowResultCarrier::from_actual_complete_input(
                    Arc::clone(&output.values),
                    Arc::clone(&self.owner.contract),
                    self.input.rows(),
                    self.data_slot.clone(),
                    output.original_carrier_stock,
                ),
            ))
        });
        self.latch(result)
    }
    fn retained_bytes(&self) -> usize {
        self.retained()
    }
    fn evaluate_evaluated<'selection>(
        &mut self,
        selection: Selection<'selection>,
        row_capacity: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'selection>, WindowEvaluationFailure> {
        if let Some(cause) = &self.first_failure {
            return Err(cause.clone());
        }
        if self.finished {
            return Err(KernelFailure::InstanceFailed.into());
        }
        let result = observed(control, |work, _| {
            if selection.batch_rows() != self.input.rows() || selection.len() > row_capacity {
                return Err(invalid(
                    "tracked BY output differs from its partition Selection or row grant",
                )
                .into());
            }
            let out = self
                .output
                .as_ref()
                .expect("open successful partition owns complete results");
            let values = if selection.len() == selection.batch_rows() {
                Arc::clone(&out.values)
            } else if selection.is_empty() {
                out.values.slice(0, 0)
            } else {
                let mut staging =
                    WindowOutputScalars::try_new(selection.len(), Arc::clone(&self.host), work)?;
                for ordinal in 0..selection.len() {
                    let row = selection
                        .row(ordinal)
                        .ok_or_else(|| invalid("tracked BY output Selection ordinal is absent"))?;
                    staging
                        .push_original_read(&out.values, row, out.resources, work)
                        .map_err(WindowEvaluationFailure::from)?;
                    work.step()?;
                }
                let resources = ScalarOutputResources::from_owned(
                    self.owner.contract.result_type().data_type(),
                    staging.values(),
                    &mut ScalarWork::new(Some(work)),
                )
                .map_err(scalar_resource_failure)?;
                let (values, staging_charge) = staging.into_parts();
                let values = with_scalar_output_operation(
                    resources,
                    Arc::clone(&self.host),
                    &self.allocator,
                    work,
                    |scalar_work| {
                        scalar::build_scalar_array(
                            self.owner.contract.result_type().data_type(),
                            values,
                            scalar_work,
                        )
                    },
                );
                drop(staging_charge);
                values.map_err(WindowEvaluationFailure::from)?.values
            };
            SelectedValues::try_new(
                selection,
                self.owner.contract.result_type().data_type(),
                values,
                Box::default(),
            )
            .map_err(|_| internal("tracked BY output violates its exact selected metadata").into())
        });
        self.latch(result)
    }
    fn finish_evaluated(
        &mut self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), WindowEvaluationFailure> {
        if let Some(cause) = &self.first_failure {
            return Err(cause.clone());
        }
        if self.finished {
            return Err(KernelFailure::InstanceFailed.into());
        }
        let result = observed(control, |_work, _| {
            self.output = None;
            Ok(())
        });
        if result.is_ok() {
            self.finished = true;
        }
        self.latch(result)
    }
}
