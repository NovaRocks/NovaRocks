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
//! Selected JSON key projection over the one canonical owner.
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::arrow_result_custody::{
    custody_type_metadata_upper_bound, retain_result_backing_direct,
};
use crate::json_canonical::{JsonCanonicalKey, normalize_json_key_in};
use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::opaque_memory::OpaqueRetainedCharge;
use crate::{
    AggregateStateAllocator, FunctionArgumentType, FunctionBindingSelection, FunctionResultType,
    KernelEvaluationControl, KernelFailure, ScalarCallContract, ScalarCallInput, SelectedValues,
};
use allocator_api2::vec::Vec as HostVec;
use arrow_array::{Array, ArrayRef, StringArray, builder::StringBuilder};
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
        && source.logical_type() == ValueLogicalType::Json
        && source.data_type() == &DataType::Utf8
        && target.logical_type() == ValueLogicalType::Physical
        && target.data_type() == &DataType::Utf8
        && target.nullable() == source.nullable()
}
pub(super) fn validate_profile(
    contract: &ScalarCallContract,
    mut observe: impl FnMut() -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    observe()?;
    if contract.effects().argument_control != ArgumentControl::Eager
        || !original_native_profile(
            contract.selected(),
            contract.call().logical_argument_count(),
        )
    {
        return Err(invalid(
            "JSON canonical key requires one exact Json/Utf8 input and matching-nullability plain Utf8 output",
        ));
    }
    observe()
}
fn add(a: usize, b: usize) -> Result<usize, KernelFailure> {
    a.checked_add(b).ok_or(KernelFailure::ResourceExhausted)
}
fn mul(a: usize, b: usize) -> Result<usize, KernelFailure> {
    a.checked_mul(b).ok_or(KernelFailure::ResourceExhausted)
}

pub(super) fn evaluate<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
    allocator: &HostAggregateAllocator,
    host: &Arc<dyn AggregateStateAllocator>,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    validate_profile(input.contract(), || control.checkpoint(1))?;
    let [argument] = input.arguments() else {
        return Err(invalid("JSON canonical key requires one argument"));
    };
    let source = argument
        .array()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            internal("JSON canonical input carrier differs from its frozen Utf8 declaration")
        })?;
    let selection = input.selection();
    let mut keys: HostVec<Option<JsonCanonicalKey>, _> = HostVec::new_in(allocator.clone());
    keys.try_reserve_exact(selection.len())
        .map_err(|_| allocator.take_failure())?;
    let mut total = 0usize;
    for (ordinal, row) in selection.iter().enumerate() {
        control.checkpoint(1)?;
        let row = argument.value_row(ordinal, row);
        if row >= source.len() {
            return Err(invalid(
                "JSON canonical selected input address is outside Utf8",
            ));
        }
        let key = if source.is_null(row) {
            None
        } else {
            let key = normalize_json_key_in(source.value(row), allocator.clone(), control)?;
            total = add(total, key.as_bytes().len())?;
            Some(key)
        };
        keys.push(key);
    }
    // This is the actual carrier's i32 offset boundary, not a JSON quota.
    if i32::try_from(total).is_err() {
        return Err(KernelFailure::from_domain(
            crate::json_canonical::JsonKeyOutputExtent { bytes: total },
        ));
    }
    let offsets = mul(add(selection.len(), 1)?, size_of::<i32>())?;
    let validity = add(add(selection.len(), 7)? / 8, 63)? / 64 * 64;
    // StringBuilder::with_capacity reserves exact values/offsets capacities;
    // lazy validity is rounded to64 bytes. finish resets one i32 offset. The
    // existing custody author covers the original/wrapping metadata overlap.
    let peak = add(
        add(add(total, offsets)?, validity)?,
        add(
            custody_type_metadata_upper_bound(1)?,
            4 * size_of::<i32>() + 2 * crate::MAX_ROW_ERROR_MESSAGE_BYTES,
        )?,
    )?;
    let charge = OpaqueRetainedCharge::try_new(Arc::clone(host))?;
    let mut reservation = charge.reserve_operation(peak)?;
    control.checkpoint(0)?;
    let mut builder = StringBuilder::with_capacity(selection.len(), total);
    for key in &keys {
        control.checkpoint(1)?;
        match key {
            Some(key) => builder.append_value(key.as_str()),
            None => builder.append_null(),
        }
    }
    let values: ArrayRef = Arc::new(builder.finish());
    drop(builder);
    drop(keys);
    let mut work = EvaluationCheckpoints::new(control);
    let retained = retain_result_backing_direct(
        values,
        charge,
        &mut reservation,
        allocator.clone(),
        &mut work,
    )?;
    let result = SelectedValues::try_new_observed::<KernelFailure>(
        selection,
        input.contract().result_type().data_type(),
        retained.values,
        Box::default(),
        || work.step(),
    )?;
    work.flush()?;
    Ok(result)
}
