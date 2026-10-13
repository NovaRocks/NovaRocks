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

//! Observed exact value types and selected carrier checks shared by lifecycle ABIs.

use crate::kernel_control::{compile_failure, internal, invalid, type_failure};
use crate::{
    EvaluatedArgument, KernelEvaluationControl, KernelFailure, MAX_UNOBSERVED_KERNEL_WORK,
    Selection,
};
use arrow_array::types::{
    Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{Array, DictionaryArray, RunArray, UnionArray};
use arrow_schema::DataType;
use novarocks_type_contract::{CompileCheckpoints, FunctionValueType};

#[cfg(test)]
mod constant_tests;
#[cfg(test)]
mod selected_null_tests;

/// Bounded runtime observation shared by builtins and external pure providers.
/// The first refusal is latched through all subsequent operations. This scope
/// observes owned work; it neither grants memory nor observes library internals.
pub struct EvaluationCheckpoints<'a> {
    control: &'a dyn KernelEvaluationControl,
    pending: u32,
    refusal: Option<KernelFailure>,
}
impl<'a> EvaluationCheckpoints<'a> {
    pub fn new(control: &'a dyn KernelEvaluationControl) -> Self {
        Self {
            control,
            pending: 0,
            refusal: None,
        }
    }
    pub fn step(&mut self) -> Result<(), KernelFailure> {
        if let Some(refusal) = &self.refusal {
            return Err(refusal.clone());
        }
        self.pending += 1;
        if self.pending == MAX_UNOBSERVED_KERNEL_WORK {
            if let Err(refusal) = self.control.checkpoint(self.pending) {
                self.refusal = Some(refusal.clone());
                return Err(refusal);
            }
            self.pending = 0;
        }
        Ok(())
    }
    pub fn flush(&mut self) -> Result<(), KernelFailure> {
        if let Some(refusal) = &self.refusal {
            return Err(refusal.clone());
        }
        if let Err(refusal) = self.control.checkpoint(self.pending) {
            self.refusal = Some(refusal.clone());
            return Err(refusal);
        }
        self.pending = 0;
        Ok(())
    }
    pub fn finish(self) -> Result<(), KernelFailure> {
        if let Some(refusal) = self.refusal {
            return Err(refusal);
        }
        self.control.checkpoint(self.pending)
    }

    /// Preserve an originating observation refusal; otherwise observe the
    /// ordinary completion or failure tail using this same work scope.
    pub fn finish_result<T>(self, result: Result<T, KernelFailure>) -> Result<T, KernelFailure> {
        if let Some(refusal) = self.refusal {
            return Err(refusal);
        }
        if matches!(
            result,
            Err(KernelFailure::Cancelled
                | KernelFailure::DeadlineExceeded
                | KernelFailure::ResourceExhausted)
        ) {
            return result;
        }
        self.control.checkpoint(self.pending)?;
        result
    }
}

/// Shared selected-carrier validation; logical identity belongs to the checked
/// call, while encoded SQL NULLs are inspected without materializing a bitmap.
pub fn validate_argument_observed(
    argument: EvaluatedArgument<'_>,
    selection: Selection<'_>,
    value_type: &FunctionValueType,
    control: &dyn KernelEvaluationControl,
) -> Result<(), KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        argument.validate_shape_observed::<KernelFailure>(selection, || work.step())?;
        if let EvaluatedArgument::Constant(value) = argument {
            work.step()?;
            if !constant_type_matches(value.value_type(), value_type) {
                return Err(invalid(
                    "constant argument differs from its exact logical type or nullability",
                ));
            }
        }
        if !novarocks_type_contract::arrow_data_types_exact_observed::<KernelFailure>(
            argument.array().data_type(),
            &value_type.data_type,
            || work.step(),
        )? {
            return Err(invalid(
                "evaluated argument differs from its exact selected type",
            ));
        }
        work.step()?;
        if !value_type.nullable {
            for (ordinal, row) in selection.iter().enumerate() {
                if logical_is_null(
                    argument.array().as_ref(),
                    argument.value_row(ordinal, row),
                    1,
                    &mut work,
                )? {
                    return Err(invalid("non-null argument contains a selected SQL NULL"));
                }
            }
        }
        Ok(())
    })();
    if work.refusal.is_some() {
        return result;
    }
    work.finish()?;
    result
}

/// One original Constant rule, shared with numerical request preparation.
/// Physical comparison and selected NULL addressing retain their own authors.
pub(crate) fn constant_type_matches(
    actual: &FunctionValueType,
    expected: &FunctionValueType,
) -> bool {
    actual.logical_type == expected.logical_type && (!actual.nullable || expected.nullable)
}

/// Inspect selected SQL NULLs through the original carrier-addressing owner.
/// The expression controller may inspect error placeholders while forming a
/// call mask. This does not authorize passing unresolved errors to a kernel.
/// Each successful bounded row callback is charged in this same work scope;
/// callers flush their outer scope first and do not keep a concurrent counter
/// for the callback's row work. Additional callback traversal needs its own
/// explicit control boundary.
pub fn visit_selected_nulls(
    argument: EvaluatedArgument<'_>,
    selection: Selection<'_>,
    control: &dyn KernelEvaluationControl,
    mut visit: impl FnMut(usize, usize, bool) -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let mut visitor_refused = false;
    let result = (|| {
        if let EvaluatedArgument::SelectedColumn(output) = argument {
            if !output
                .selection()
                .same_rows_observed(selection, || work.step())?
                || output.values().len() != selection.len()
            {
                return Err(invalid(
                    "NULL inspection requires the exact selected carrier",
                ));
            }
        } else {
            argument.validate_shape_observed::<KernelFailure>(selection, || work.step())?;
        }
        for (ordinal, row) in selection.iter().enumerate() {
            let is_null = logical_is_null(
                argument.array().as_ref(),
                argument.value_row(ordinal, row),
                1,
                &mut work,
            )?;
            if let Err(refusal) = visit(ordinal, row, is_null) {
                visitor_refused = true;
                return Err(refusal);
            }
            work.step()?;
        }
        Ok(())
    })();
    // A callback refusal is primary regardless of its error category. Ordinary
    // shape/address failures still observe the completed work before publication.
    if visitor_refused || work.refusal.is_some() {
        return result;
    }
    work.finish()?;
    result
}

/// Logical NULL checks never materialize Arrow's dictionary/union/run-end
/// logical-null bitmap. Every visited row/type node has bounded work control.
pub(crate) fn logical_is_null(
    array: &dyn Array,
    row: usize,
    depth: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<bool, KernelFailure> {
    work.step()?;
    if depth > novarocks_type_contract::MAX_VALUE_TYPE_DEPTH || row >= array.len() {
        return Err(internal("kernel value has invalid nested row addressing"));
    }
    if array.is_null(row) {
        return Ok(true);
    }
    match array.data_type() {
        DataType::Null => Ok(true),
        DataType::Dictionary(key, _) => {
            macro_rules! dictionary {
                ($key:ty) => {{
                    let array = array
                        .as_any()
                        .downcast_ref::<DictionaryArray<$key>>()
                        .ok_or_else(|| {
                            internal("kernel dictionary carrier differs from its type")
                        })?;
                    match array.key(row) {
                        Some(key) => logical_is_null(array.values().as_ref(), key, depth + 1, work),
                        None => Ok(true),
                    }
                }};
            }
            match key.as_ref() {
                DataType::Int8 => dictionary!(Int8Type),
                DataType::Int16 => dictionary!(Int16Type),
                DataType::Int32 => dictionary!(Int32Type),
                DataType::Int64 => dictionary!(Int64Type),
                DataType::UInt8 => dictionary!(UInt8Type),
                DataType::UInt16 => dictionary!(UInt16Type),
                DataType::UInt32 => dictionary!(UInt32Type),
                DataType::UInt64 => dictionary!(UInt64Type),
                _ => Err(internal("kernel dictionary key type is invalid")),
            }
        }
        DataType::RunEndEncoded(run_ends, _) => {
            macro_rules! run {
                ($key:ty) => {{
                    let array = array
                        .as_any()
                        .downcast_ref::<RunArray<$key>>()
                        .ok_or_else(|| internal("kernel run-end carrier differs from its type"))?;
                    logical_is_null(
                        array.values().as_ref(),
                        array.get_physical_index(row),
                        depth + 1,
                        work,
                    )
                }};
            }
            match run_ends.data_type() {
                DataType::Int16 => run!(Int16Type),
                DataType::Int32 => run!(Int32Type),
                DataType::Int64 => run!(Int64Type),
                _ => Err(internal("kernel run-end index type is invalid")),
            }
        }
        DataType::Union(fields, _) => {
            let array = array
                .as_any()
                .downcast_ref::<UnionArray>()
                .ok_or_else(|| internal("kernel union carrier differs from its type"))?;
            let type_id = array.type_id(row);
            if !fields.iter().any(|(id, _)| id == type_id) {
                return Err(internal("kernel union type id is invalid"));
            }
            logical_is_null(
                array.child(type_id).as_ref(),
                array.value_offset(row),
                depth + 1,
                work,
            )
        }
        _ => Ok(false),
    }
}

/// Validate a complete pure function value type in the caller's existing work
/// scope. Local compilation uses the same signature/logical/metadata limits as
/// kernel preparation, without reconstructing a second type resource policy.
pub fn validate_type_observed(
    value: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    value
        .logical_type
        .validate_carrier(&value.data_type)
        .map_err(type_failure)?;
    use novarocks_type_contract::*;
    validate_value_type_structure_observed(&value.data_type, |visit| {
        work.step().map_err(compile_failure)?;
        match visit {
            ValueTypeVisit::TypeNode(ty) => {
                if let DataType::Timestamp(_, Some(zone)) = ty
                    && zone.len() > MAX_ARROW_TIMESTAMP_TIMEZONE_BYTES
                {
                    return Err(KernelFailure::ResourceExhausted);
                }
                validate_arrow_carrier_parameters_observed::<KernelFailure>(ty, || {
                    work.step().map_err(compile_failure)
                })?;
            }
            ValueTypeVisit::Field(field) => {
                if field.name().len() > MAX_ARROW_FIELD_NAME_BYTES
                    || field.metadata().len() > MAX_ARROW_FIELD_METADATA_ENTRIES
                {
                    return Err(KernelFailure::ResourceExhausted);
                }
                let mut bytes = 0usize;
                for (key, value) in field.metadata() {
                    work.step().map_err(compile_failure)?;
                    if key.len() > MAX_ARROW_FIELD_METADATA_KEY_BYTES
                        || value.len() > MAX_ARROW_FIELD_METADATA_VALUE_BYTES
                    {
                        return Err(KernelFailure::ResourceExhausted);
                    }
                    bytes += key.len() + value.len();
                    if bytes > MAX_ARROW_FIELD_METADATA_BYTES {
                        return Err(KernelFailure::ResourceExhausted);
                    }
                }
            }
            _ => {}
        }
        Ok(())
    })
}
