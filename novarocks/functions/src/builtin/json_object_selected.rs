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
//! Selected adapter for one shared whole-call JSONObject computation.
use super::scalar_invocation_data::{ScalarDataSlot, ScalarInvocationFailure};
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_scalar::{
    AggScalarValue, ScalarReadFailure, ScalarReadFailureSink, ScalarReadObservation,
    scalar_from_array_with_failure,
};
use crate::arrow_result_custody::retain_result_backing_direct;
use crate::json_object_core::{JsonObjectArguments, JsonObjectFailure, JsonObjectPort};
use crate::json_object_resources::JsonObjectBatchResources;
use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::opaque_memory::OpaqueRetainedCharge;
use crate::{
    AggregateStateAllocator, EvaluatedArgument, FunctionArgumentType, FunctionBindingSelection,
    FunctionResultType, KernelEvaluationControl, KernelFailure, ScalarCallContract,
    ScalarCallInput, SelectedValues,
};
use arrow_array::ArrayRef;
use arrow_schema::DataType;
use novarocks_type_contract::{ArgumentControl, FunctionNullBehavior, ValueLogicalType};
use std::{alloc::Layout, sync::Arc};

pub(super) fn original_native_profile(selected: &FunctionBindingSelection, count: usize) -> bool {
    let FunctionResultType::Scalar(target) = &selected.result_type else {
        return false;
    };
    count != 0
        && count == selected.argument_types.len()
        && selected.aggregate.is_none()
        && selected
            .argument_types
            .iter()
            .all(|arg| matches!(arg, FunctionArgumentType::Value(_)))
        && target.logical_type == ValueLogicalType::Json
        && target.data_type == DataType::Utf8
        && target.nullable
}
pub(super) fn validate_profile(
    contract: &ScalarCallContract,
    mut observe: impl FnMut() -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    observe()?;
    if contract.effects().argument_control != ArgumentControl::Eager
        || contract.effects().null_behavior != FunctionNullBehavior::CalledOnNull
        || !original_native_profile(
            contract.selected(),
            contract.call().logical_argument_count(),
        )
    {
        return Err(invalid(
            "json_object requires its original independent value arguments to nullable Json profile",
        ));
    }
    observe()
}
struct Arguments<'a>(&'a [EvaluatedArgument<'a>]);
impl JsonObjectArguments for Arguments<'_> {
    fn len(&self) -> usize {
        self.0.len()
    }
    fn array(&self, index: usize) -> &ArrayRef {
        self.0[index].array()
    }
    fn value_row(&self, index: usize, ordinal: usize, row: usize) -> usize {
        self.0[index].value_row(ordinal, row)
    }
}
struct Port<'w, 'c> {
    work: &'w mut EvaluationCheckpoints<'c>,
    body_grant: usize,
}
impl ScalarReadFailureSink for Port<'_, '_> {
    type Error = JsonObjectFailure<KernelFailure>;
    fn read_data(&mut self, failure: ScalarReadFailure<'_>) -> Self::Error {
        JsonObjectFailure::OriginalData(failure.to_string())
    }
    fn observe(&mut self, event: ScalarReadObservation) -> Result<(), Self::Error> {
        match event {
            ScalarReadObservation::Step => self.work.step(),
            ScalarReadObservation::OpaqueBoundary => self.work.flush(),
        }
        .map_err(JsonObjectFailure::Control)
    }
    fn reserve_scalar_copy(
        &mut self,
        elements: usize,
        element_bytes: usize,
        alignment: usize,
    ) -> Result<(), Self::Error> {
        let bytes = elements
            .checked_mul(element_bytes)
            .ok_or(JsonObjectFailure::Control(KernelFailure::ResourceExhausted))?;
        let layout = Layout::from_size_align(bytes, alignment)
            .map_err(|_| JsonObjectFailure::Control(KernelFailure::ResourceExhausted))?;
        if layout.size() > self.body_grant {
            return Err(JsonObjectFailure::Control(internal(
                "json_object scalar copy exceeds its pre-operation geometry",
            )));
        }
        // The full simultaneous copy prefix was admitted before the reader.
        // This check grants nothing and never opens an allocator journal.
        Ok(())
    }
}
impl JsonObjectPort for Port<'_, '_> {
    type Control = KernelFailure;
    fn step(&mut self) -> Result<(), KernelFailure> {
        self.work.step()
    }
    fn boundary(&mut self) -> Result<(), KernelFailure> {
        self.work.flush()
    }
    fn read_scalar(
        &mut self,
        array: &ArrayRef,
        row: usize,
    ) -> Result<Option<AggScalarValue>, JsonObjectFailure<KernelFailure>> {
        scalar_from_array_with_failure(array, row, self)
    }
    fn debug_formatting_failure(&mut self) -> KernelFailure {
        internal("json_object original Debug destination failed")
    }
}

pub(super) fn evaluate<'a>(
    input: ScalarCallInput<'_, 'a>,
    contract: &Arc<ScalarCallContract>,
    control: &dyn KernelEvaluationControl,
    allocator: &HostAggregateAllocator,
    host: &Arc<dyn AggregateStateAllocator>,
) -> Result<SelectedValues<'a>, ScalarInvocationFailure> {
    control.checkpoint(0)?;
    if !std::ptr::eq(input.contract(), contract.as_ref()) {
        return Err(invalid("json_object invocation replaced its immutable contract").into());
    }
    validate_profile(input.contract(), || control.checkpoint(1))?;
    let selection = input.selection();
    let arguments = Arguments(input.arguments());
    let mut resources = JsonObjectBatchResources::default();
    for (ordinal, row) in selection.iter().enumerate() {
        control.checkpoint(1)?;
        resources.add_row(&arguments, ordinal, row, control)?;
    }
    let body_grant = resources.body_grant(selection.len())?;
    let diagnostic_grant = resources.diagnostic_grant()?;
    let source_bytes = if selection.is_all() {
        0
    } else {
        Layout::array::<usize>(selection.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?
            .size()
    };
    let charge = OpaqueRetainedCharge::try_new(Arc::clone(host))?;
    control.checkpoint(0)?;
    let mut reservation = charge.reserve_operation(body_grant)?;
    let diagnostic = charge.reserve_operation(diagnostic_grant)?;
    let source = if source_bytes == 0 {
        None
    } else {
        Some(charge.reserve_operation(source_bytes)?)
    };
    let result = {
        // Only this inline observer can clone one bounded refusal. Its backing
        // is covered by body_grant and dies before that reservation is released.
        let mut work = EvaluationCheckpoints::new(control);
        (|| {
            let slot = ScalarDataSlot::prepare_direct_for_actual_invocation(
                Arc::clone(contract),
                selection,
                allocator,
                diagnostic,
                source,
                &mut work,
            )?;
            let values = match crate::json_object_core::evaluate_selected(
                &arguments,
                selection,
                &mut Port {
                    work: &mut work,
                    body_grant,
                },
            ) {
                Ok(values) => values,
                Err(JsonObjectFailure::OriginalData(message)) => {
                    return Err(ScalarInvocationFailure::Data(
                        slot.publish_original(message),
                    ));
                }
                Err(JsonObjectFailure::Control(cause)) => {
                    return Err(ScalarInvocationFailure::Kernel(cause));
                }
            };
            drop(slot);
            let retained = retain_result_backing_direct(
                values,
                charge,
                &mut reservation,
                allocator.clone(),
                &mut work,
            )?;
            let values = SelectedValues::try_new_observed::<KernelFailure>(
                selection,
                &input.contract().result_type().data_type,
                retained.values,
                Box::default(),
                || work.step(),
            )?;
            work.flush()?;
            Ok(values)
        })()
    };
    drop(reservation);
    result
}
