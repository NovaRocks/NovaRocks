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

//! Exact window calls and complete partition inputs. Execution owns partition
//! gathering, peers, frame membership, state, lifecycle and output selection.

use crate::kernel_control::{compile_failure, internal, invalid};
use crate::kernel_input::validate_argument_observed;
use crate::{
    AggregateCallContract, AggregateKernelPhase, AggregateOrderKey, EvaluatedArgument,
    FunctionArgumentType, FunctionCallContract, FunctionResultType, KernelEvaluationControl,
    KernelFailure, SelectedValues, Selection,
};
use novarocks_type_contract::{
    ArgumentControl, CompileCheckpoints, CompilePhase, FunctionInstanceState,
    FunctionIntrinsicRowError, FunctionKind, FunctionValueType, PureCompileControl, WindowBound,
    WindowFrame, WindowFrameUnits,
};
use std::sync::Arc;

/// Immutable prepared frame options for the currently supported integer
/// offsets. None preserves the absence of an explicit frame; it is never
/// replaced here by a default frame. RANGE offsets require a distinct exact
/// implementation and are rejected, rather than guessed as row counts.
/// GROUPS and exclusions are preserved vocabulary, not a capability promise.
/// The exact installed specializer must check supported frame and NULL options.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WindowCallOptions {
    frame: Option<WindowFrame<u64>>,
    ignore_nulls: bool,
}
impl WindowCallOptions {
    pub fn try_new(
        frame: Option<WindowFrame<u64>>,
        ignore_nulls: bool,
        control: &dyn PureCompileControl,
    ) -> Result<Self, KernelFailure> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(compile_failure)?;
        if let Some(frame) = frame {
            if matches!(frame.start, WindowBound::UnboundedFollowing)
                || matches!(frame.end, WindowBound::UnboundedPreceding)
                || bound_position(frame.start) > bound_position(frame.end)
            {
                return Err(invalid("window frame start follows its end"));
            }
            for bound in [frame.start, frame.end] {
                if let WindowBound::Preceding(offset) | WindowBound::Following(offset) = bound {
                    if offset == 0 {
                        return Err(invalid("zero window offset requires CURRENT ROW"));
                    }
                    if frame.units == WindowFrameUnits::Range {
                        return Err(invalid("RANGE offset has no exact prepared implementation"));
                    }
                }
                work.step().map_err(compile_failure)?;
            }
        }
        work.finish().map_err(compile_failure)?;
        Ok(Self {
            frame,
            ignore_nulls,
        })
    }
    pub const fn frame(&self) -> Option<&WindowFrame<u64>> {
        self.frame.as_ref()
    }
    pub const fn ignore_nulls(&self) -> bool {
        self.ignore_nulls
    }
}
fn bound_position(bound: WindowBound<u64>) -> i128 {
    match bound {
        WindowBound::UnboundedPreceding => i128::MIN,
        WindowBound::Preceding(offset) => -i128::from(offset),
        WindowBound::CurrentRow => 0,
        WindowBound::Following(offset) => i128::from(offset),
        WindowBound::UnboundedFollowing => i128::MAX,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum WindowCallSource {
    Window(Arc<FunctionCallContract>),
    Aggregate(Arc<AggregateCallContract>),
}

/// Immutable call facts, not proof that an implementation supports these
/// options. In particular, FunctionCallContract's refinement receipt does not
/// cover frame/IGNORE NULLS; actual window specialization must check them.
/// Aggregate OVER retains its exact Single aggregate binding, DISTINCT and
/// separate function ORDER BY channels; OVER ordering remains node-owned.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WindowCallContract {
    source: WindowCallSource,
    options: WindowCallOptions,
}
impl WindowCallContract {
    pub fn try_window(
        call: Arc<FunctionCallContract>,
        options: WindowCallOptions,
        control: &dyn PureCompileControl,
    ) -> Result<Self, KernelFailure> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(compile_failure)?;
        if call.kind() != FunctionKind::Window
            || call.effects().argument_control != ArgumentControl::Window
            || call.effects().instance_state != FunctionInstanceState::WindowPartition
            || call.effects().own_row_error != FunctionIntrinsicRowError::NotRowEvaluated
            || !matches!(&call.selected().result_type, FunctionResultType::Scalar(_))
            || call.logical_argument_count() != call.selected().argument_types.len()
        {
            return Err(invalid(
                "window preparation requires its exact window channels",
            ));
        }
        for channel in &call.selected().argument_types {
            if !matches!(channel, FunctionArgumentType::Value(_)) {
                return Err(invalid("window argument channel cannot be a lambda"));
            }
            work.step().map_err(compile_failure)?;
        }
        work.finish().map_err(compile_failure)?;
        Ok(Self {
            source: WindowCallSource::Window(call),
            options,
        })
    }
    pub fn try_aggregate(
        call: Arc<AggregateCallContract>,
        options: WindowCallOptions,
        control: &dyn PureCompileControl,
    ) -> Result<Self, KernelFailure> {
        control
            .checkpoint(CompilePhase::FunctionSpecialization, 0)
            .map_err(compile_failure)?;
        if call.phase() != AggregateKernelPhase::Single || call.state_input_type().is_some() {
            return Err(invalid("aggregate OVER requires its exact Single phase"));
        }
        Ok(Self {
            source: WindowCallSource::Aggregate(call),
            options,
        })
    }
    pub fn call(&self) -> &Arc<FunctionCallContract> {
        match &self.source {
            WindowCallSource::Window(call) => call,
            WindowCallSource::Aggregate(call) => call.call(),
        }
    }
    pub const fn options(&self) -> &WindowCallOptions {
        &self.options
    }
    pub fn aggregate(&self) -> Option<&Arc<AggregateCallContract>> {
        match &self.source {
            WindowCallSource::Window(_) => None,
            WindowCallSource::Aggregate(call) => Some(call),
        }
    }
    pub fn logical_argument_types(&self) -> impl ExactSizeIterator<Item = &FunctionValueType> {
        self.call().selected().argument_types[..self.call().logical_argument_count()]
            .iter()
            .map(value_type)
    }
    pub fn order_argument_types(&self) -> impl ExactSizeIterator<Item = &FunctionValueType> {
        self.call().selected().argument_types[self.call().logical_argument_count()..]
            .iter()
            .map(value_type)
    }
    pub fn function_order_keys(&self) -> &[AggregateOrderKey] {
        self.aggregate().map_or(&[], |call| call.order_keys())
    }
    pub fn result_type(&self) -> &FunctionValueType {
        match &self.call().selected().result_type {
            FunctionResultType::Scalar(value) => value,
            _ => unreachable!("checked window scalar result"),
        }
    }
}
fn value_type(argument: &FunctionArgumentType) -> &FunctionValueType {
    match argument {
        FunctionArgumentType::Value(value) => value,
        _ => unreachable!("checked window value channel"),
    }
}

/// Borrowed complete input domain, independent of later output Selection.
/// All logical and function ORDER BY columns cover every partition row;
/// Scalar is explicit broadcast, not a promise that this parameter is constant
/// at preparation. Each overload chooses its exact specialization constants.
/// Sparse selected columns and errored columns cannot impersonate a
/// complete partition; a checked dense error-free SelectedColumn is legal.
/// Upstream required input failures propagate at the host.
///
/// Equal lengths do not establish partition/source identity. Execution supplies
/// the exact gathered partition and owns peer/frame facts; this checker cannot
/// authorize gathering, sorting, CASE guards or dropping upstream work/errors.
#[derive(Clone, Copy, Debug)]
pub struct FullPartitionWindowInput<'call, 'a> {
    contract: &'call WindowCallContract,
    partition_rows: usize,
    logical_arguments: &'a [EvaluatedArgument<'a>],
    order_arguments: &'a [EvaluatedArgument<'a>],
}
impl<'call, 'a> FullPartitionWindowInput<'call, 'a> {
    pub fn try_new(
        contract: &'call WindowCallContract,
        partition_rows: usize,
        logical_arguments: &'a [EvaluatedArgument<'a>],
        order_arguments: &'a [EvaluatedArgument<'a>],
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, KernelFailure> {
        validate_full_window_arguments(
            contract,
            partition_rows,
            logical_arguments,
            order_arguments,
            control,
        )?;
        Ok(Self {
            contract,
            partition_rows,
            logical_arguments,
            order_arguments,
        })
    }
    pub const fn contract(self) -> &'call WindowCallContract {
        self.contract
    }
    pub const fn partition_rows(self) -> usize {
        self.partition_rows
    }
    pub const fn logical_arguments(self) -> &'a [EvaluatedArgument<'a>] {
        self.logical_arguments
    }
    pub const fn order_arguments(self) -> &'a [EvaluatedArgument<'a>] {
        self.order_arguments
    }
    /// Bounds only. This output Selection never authorizes skipping partition
    /// setup/state/input work or suppressing a required upstream failure.
    pub fn validate_output_selection(self, selection: Selection<'_>) -> Result<(), KernelFailure> {
        if selection.batch_rows() != self.partition_rows {
            return Err(invalid(
                "window output Selection addresses another partition",
            ));
        }
        Ok(())
    }
}

/// ONE exact full-input validator, shared by original partition loans and
/// explicitly authored whole-invocation loans. Neither grants permission to
/// gather, reorder, reuse an occurrence or skip required argument failures.
pub(crate) fn validate_full_window_arguments(
    contract: &WindowCallContract,
    partition_rows: usize,
    logical_arguments: &[EvaluatedArgument<'_>],
    order_arguments: &[EvaluatedArgument<'_>],
    control: &dyn KernelEvaluationControl,
) -> Result<(), KernelFailure> {
    control.checkpoint(0)?;
    if logical_arguments.len() != contract.logical_argument_types().len()
        || order_arguments.len() != contract.order_argument_types().len()
    {
        return Err(invalid(
            "window partition differs from its exact logical/order channels",
        ));
    }
    let selection = Selection::all(partition_rows);
    for (argument, value_type) in logical_arguments
        .iter()
        .zip(contract.logical_argument_types())
        .chain(order_arguments.iter().zip(contract.order_argument_types()))
    {
        validate_argument_observed(*argument, selection, value_type, control)?;
    }
    Ok(())
}

/// An output projection bound to one complete partition input borrow. Sparse
/// or empty output never reduces the input/setup/state/error responsibility.
/// This view does not prove that peer/frame or instance facts came from that
/// partition; the host must bind those exact owners before invoking a kernel.
#[derive(Clone, Copy, Debug)]
pub struct WindowOutputProjection<'input, 'call, 'a, 'selection> {
    input: &'input FullPartitionWindowInput<'call, 'a>,
    selection: Selection<'selection>,
}
impl<'input, 'call, 'a, 'selection> WindowOutputProjection<'input, 'call, 'a, 'selection> {
    pub fn try_new(
        input: &'input FullPartitionWindowInput<'call, 'a>,
        selection: Selection<'selection>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, KernelFailure> {
        control.checkpoint(0)?;
        input.validate_output_selection(selection)?;
        Ok(Self { input, selection })
    }
    pub const fn input(self) -> &'input FullPartitionWindowInput<'call, 'a> {
        self.input
    }
    pub const fn selection(self) -> Selection<'selection> {
        self.selection
    }
    /// Check produced values before exposing them. Capacity is an actual host
    /// row grant, not a memory allowance or protection for prior allocation.
    /// Window/aggregate failures have no scalar row-error masking channel.
    /// Exact logical identity belongs to the checked result contract; Arrow
    /// carrier equality cannot authenticate an independent logical type tag.
    pub fn validate_result(
        self,
        result: &SelectedValues<'_>,
        capacity: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        validate_window_result(
            self.input.contract,
            self.selection,
            result,
            capacity,
            control,
        )
    }
}

pub(crate) fn validate_window_result(
    contract: &WindowCallContract,
    selection: Selection<'_>,
    result: &SelectedValues<'_>,
    capacity: usize,
    control: &dyn KernelEvaluationControl,
) -> Result<(), KernelFailure> {
    let observation = crate::kernel_control::KernelControlObservation::new(control);
    observation.checkpoint(0)?;
    if result.values().len() > capacity {
        return Err(internal("window output exceeds its host row grant"));
    }
    let validated = validate_argument_observed(
        EvaluatedArgument::SelectedColumn(result),
        selection,
        contract.result_type(),
        &observation,
    )
    .map_err(|failure| match failure {
        KernelFailure::InvalidProgram(diagnostic) => KernelFailure::Internal(diagnostic),
        failure => failure,
    });
    observation.finish(validated)
}

#[cfg(test)]
mod tests;
