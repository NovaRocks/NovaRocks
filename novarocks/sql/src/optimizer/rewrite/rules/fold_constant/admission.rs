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

//! Fold policy for input carriers, prepared calls and one-row results.

use super::evaluation::is_fold_output_extent;
use crate::compiler::SqlCompileError;
use arrow::array::{ArrayRef, Decimal128Array, Decimal256Array};
use arrow::datatypes::{DataType, Decimal128Type, Decimal256Type, DecimalType, Field};
use novarocks_expression::CompiledExprSet;
use novarocks_failure_contract::SharedFailureCause;
use novarocks_functions::{
    ConstantPool, ConstantValidation, ConstantValue, KernelDiagnostic, KernelFailure,
    PreparedNullIfEquality, PreparedPureKernel, validate_function_value_type_observed,
};
use novarocks_type_contract::{
    ArgumentControl, CompileCheckpoints, CompilePhase, FunctionInstanceState, FunctionValueType,
    PureCompileControl, ValueKind, ValueLogicalType, ValueTypeRef, arrow_data_types_exact_observed,
};
use std::sync::Arc;

fn invalid_program(message: &str) -> SqlCompileError {
    SqlCompileError::CompilationSource(SharedFailureCause::new(KernelFailure::InvalidProgram(
        KernelDiagnostic::new(message),
    )))
}

/// Only root LARGEINT may be nominal. Physical FixedSizeBinary and nested
/// nominal values keep their original execution sites.
pub(super) fn fold_value_type_supported(
    value_type: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, SqlCompileError> {
    work.step()?;
    value_type
        .logical_type()
        .validate_carrier(value_type.data_type())
        .map_err(|cause| SqlCompileError::CompilationSource(SharedFailureCause::new(cause)))?;
    validate_function_value_type_observed(value_type, work).map_err(|cause| match cause {
        KernelFailure::CompileControl(control) => control.into(),
        cause => SqlCompileError::CompilationSource(SharedFailureCause::new(cause)),
    })?;
    value_kind_supported(value_type.value_type(), true, work)
}

fn value_kind_supported(
    value: &ValueTypeRef,
    root: bool,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, SqlCompileError> {
    work.step()?;
    let mut allowed = match value.kind() {
        ValueKind::Nominal(ValueLogicalType::LargeInt) => root,
        ValueKind::Nominal(_) | ValueKind::Scalar(DataType::FixedSizeBinary(_)) => false,
        _ => true,
    };
    for ordinal in 0..value.child_count() {
        work.step()?;
        let child = value
            .child_type_at(ordinal)
            .expect("immutable value node exposes every declared child");
        allowed &= value_kind_supported(child, false, work)?;
    }
    Ok(allowed)
}

/// Check every prepared call, including calls added by scalar coercion.
pub(super) fn compiled_unit_can_fold(
    compiled: &CompiledExprSet,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, SqlCompileError> {
    for call in compiled.calls().values() {
        work.step()?;
        match call.prepared() {
            PreparedPureKernel::Scalar(kernel)
                if kernel.instance_inline_allocation_bytes() == 0 => {}
            PreparedPureKernel::ControlIntrinsic(_) => {}
            PreparedPureKernel::NullIf(recipe) => {
                if let PreparedNullIfEquality::Composite(recipe) = recipe.equality()
                    && recipe.requires_allocator()
                {
                    return Ok(false);
                }
            }
            // The optional optimizer has no original evaluation allocator.
            // Ordered calls with known NULL are simplified before this step.
            PreparedPureKernel::OrderedRegexp(_) => return Ok(false),
            _ => return Ok(false),
        }
        let effects = call.call_contract().effects();
        if effects.value_stability.is_volatile()
            || effects.instance_state != FunctionInstanceState::None
            || !effects.observable_effects.is_empty()
        {
            return Ok(false);
        }
        match effects.argument_control {
            ArgumentControl::Eager
            | ArgumentControl::If
            | ArgumentControl::Coalesce
            | ArgumentControl::NullIf
            | ArgumentControl::Try
            | ArgumentControl::OrderedNull
            | ArgumentControl::NoArguments => {}
            ArgumentControl::TemporalSource(_) => return Ok(false),
            _ => return Err(invalid_program("unsupported frozen constant call control")),
        }
    }
    // Guarded primitives can use the execution allocator without an opaque
    // scalar instance. Optional folding has no equivalent grant.
    Ok(!compiled
        .recipes()
        .requires_guarded_comparison_allocator(work)?)
}

/// Admit the evaluated value using the arena declaration. Lowering can widen
/// the root's nullability, so the compiled root cannot grant result admission.
pub(super) fn admit_result(
    output: ArrayRef,
    arena_type: &FunctionValueType,
    policy: ConstantValidation,
    control: &dyn PureCompileControl,
) -> Result<Option<ConstantValue>, SqlCompileError> {
    let phase = CompilePhase::FunctionSpecialization;
    let mut work = CompileCheckpoints::try_new(control, phase)?;
    let result = (|| {
        if output.len() != 1 {
            return Err(invalid_program(&format!(
                "constant folding produced {} rows, expected exactly 1",
                output.len()
            )));
        }
        if !arrow_data_types_exact_observed::<novarocks_functions::ConstantError>(
            output.data_type(),
            arena_type.data_type(),
            || {
                work.step()
                    .map_err(novarocks_functions::ConstantError::from)
            },
        )? {
            return Err(invalid_program(
                "constant folding output differs from its frozen carrier",
            ));
        }
        if output.is_null(0) && !arena_type.nullable() {
            return Ok(None);
        }
        if !utf8_output_fits(&output, &mut work)? {
            return Ok(None);
        }
        work.flush()?;
        let fits = decimal_output_fits(&output)?;
        work.flush()?;
        if !fits {
            return Ok(None);
        }
        let field = Arc::new(Field::new(
            "literal",
            arena_type.data_type().clone(),
            arena_type.nullable(),
        ));
        let data = output.to_data();
        work.flush()?;
        let pool =
            match ConstantPool::try_new(field, arena_type.clone(), data, policy, phase, control) {
                Ok(pool) => pool,
                Err(error)
                    if error.is_local_allocation_failure() || is_fold_output_extent(&error) =>
                {
                    return Ok(None);
                }
                Err(error) => return Err(error.into()),
            };
        work.step()?;
        Ok(Some(pool.value(0)?))
    })();
    work.finish_result(result, SqlCompileError::from)
}

// Inspect the bytes without constructing a &str from malformed UTF8. Bad
// bytes decline folding, while an invalid carrier layout remains fatal.
fn utf8_output_fits(
    output: &ArrayRef,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, SqlCompileError> {
    if output.is_null(0) {
        return Ok(true);
    }
    macro_rules! inspect {
        ($array:ty) => {{
            let array = output
                .as_any()
                .downcast_ref::<$array>()
                .ok_or_else(|| invalid_program("invalid UTF8 output class"))?;
            let offsets = array.value_offsets();
            let start = usize::try_from(offsets[0])
                .map_err(|_| invalid_program("invalid UTF8 output offset"))?;
            let end = usize::try_from(offsets[1])
                .map_err(|_| invalid_program("invalid UTF8 output offset"))?;
            let bytes = array
                .value_data()
                .get(start..end)
                .ok_or_else(|| invalid_program("invalid UTF8 output range"))?;
            // Account real bounded visits before the opaque UTF8 validator.
            for byte in bytes {
                std::hint::black_box(byte);
                work.step()?;
            }
            work.flush()?;
            let valid = std::str::from_utf8(bytes).is_ok();
            work.flush()?;
            Ok(valid)
        }};
    }
    match output.data_type() {
        DataType::Utf8 => inspect!(arrow::array::StringArray),
        DataType::LargeUtf8 => inspect!(arrow::array::LargeStringArray),
        _ => Ok(true),
    }
}

fn decimal_output_fits(output: &ArrayRef) -> Result<bool, SqlCompileError> {
    if output.is_null(0) {
        return Ok(true);
    }
    match output.data_type() {
        DataType::Decimal128(precision, _) => {
            let array = output
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| {
                    invalid_program("constant folding has an invalid Decimal128 result class")
                })?;
            Ok(Decimal128Type::is_valid_decimal_precision(
                array.value(0),
                *precision,
            ))
        }
        DataType::Decimal256(precision, _) => {
            let array = output
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| {
                    invalid_program("constant folding has an invalid Decimal256 result class")
                })?;
            Ok(Decimal256Type::is_valid_decimal_precision(
                array.value(0),
                *precision,
            ))
        }
        _ => Ok(true),
    }
}

#[cfg(test)]
mod tests;
