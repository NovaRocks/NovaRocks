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
//! Actual host operation adapter; JSON math belongs solely to parse_json_core.
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::arrow_result_custody::retain_result_backing_direct;
use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::opaque_memory::OpaqueRetainedCharge;
use crate::parse_json_core::ParseJsonObserver;
use crate::parse_json_resources::ParseJsonBatchResources;
use crate::{
    AggregateStateAllocator, FunctionArgumentType, FunctionBindingSelection, FunctionResultType,
    KernelEvaluationControl, KernelFailure, ScalarCallContract, ScalarCallInput, SelectedValues,
};
use arrow_array::{Array, StringArray};
use arrow_schema::DataType;
use novarocks_type_contract::{ArgumentControl, ValueLogicalType};
use std::sync::Arc;

pub(super) fn original_native_profile(selected: &FunctionBindingSelection, count: usize) -> bool {
    let [FunctionArgumentType::Value(source)] = selected.argument_types.as_ref() else {
        return false;
    };
    let FunctionResultType::Scalar(target) = &selected.result_type else {
        return false;
    };
    count == 1
        && selected.aggregate.is_none()
        && source.logical_type == ValueLogicalType::Physical
        && source.data_type == DataType::Utf8
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
        || contract.value_argument_types().len() != 1
        || !original_native_profile(
            contract.selected(),
            contract.call().logical_argument_count(),
        )
    {
        return Err(invalid(
            "parse_json requires its original Physical Utf8 to nullable Json profile",
        ));
    }
    observe()
}
struct Observer<'w, 'c>(&'w mut EvaluationCheckpoints<'c>);
impl ParseJsonObserver for Observer<'_, '_> {
    type Error = KernelFailure;
    fn step(&mut self) -> Result<(), KernelFailure> {
        self.0.step()
    }
    fn boundary(&mut self) -> Result<(), KernelFailure> {
        self.0.flush()
    }
}
pub(super) fn evaluate<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
    allocator: &HostAggregateAllocator,
    host: &Arc<dyn AggregateStateAllocator>,
) -> Result<SelectedValues<'a>, KernelFailure> {
    // Preflight borrows lengths only. Forward refusals directly, without an
    // observer Mutex or diagnostic clones before operation admission.
    control.checkpoint(0)?;
    validate_profile(input.contract(), || control.checkpoint(1))?;
    let [argument] = input.arguments() else {
        return Err(invalid("parse_json requires one evaluated argument"));
    };
    let array = argument
        .array()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| internal("parse_json canonical Utf8 carrier differs"))?;
    let selection = input.selection();
    let mut resources = ParseJsonBatchResources::default();
    for (ordinal, batch_row) in selection.iter().enumerate() {
        control.checkpoint(1)?;
        let row = argument.value_row(ordinal, batch_row);
        if row >= array.len() {
            return Err(internal("parse_json selected address exceeds Utf8"));
        }
        if !array.is_null(row) {
            resources.add_row(array.value(row).len())?;
        }
    }
    let peak = resources.peak(selection.len())?;
    let charge = OpaqueRetainedCharge::try_new(Arc::clone(host))?;
    control.checkpoint(0)?;
    // Actual host refusal happens before StringBuilder, serde or renderer.
    let mut reservation = charge.reserve_operation(peak)?;
    let result = {
        // One inline checkpoint author, with no Mutex allocation. Its bounded
        // diagnostic clone is covered by the preceding operation envelope.
        let mut work = EvaluationCheckpoints::new(control);
        (|| {
            work.flush()?;
            let values = crate::parse_json_core::evaluate_selected(
                array,
                selection,
                |ordinal, row| argument.value_row(ordinal, row),
                &mut Observer(&mut work),
            )?;
            // All parse trees, diagnostic Strings and renderer scratch are gone.
            // Retain real immutable backing under the SAME already-granted host.
            let retained = retain_result_backing_direct(
                values,
                charge,
                &mut reservation,
                allocator.clone(),
                &mut work,
            )?;
            let result = SelectedValues::try_new_observed::<KernelFailure>(
                selection,
                &input.contract().result_type().data_type,
                retained.values,
                Box::default(),
                || work.step(),
            )?;
            work.flush()?;
            Ok(result)
        })()
        // work's latched diagnostic dies before the operation reservation.
    };
    drop(reservation);
    // No host/control refusal receives a callback or an error clone footer.
    result
}
