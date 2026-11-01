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

//! The original safe Arrow default conversion on complete logical partition rows.
//! This private prerequisite is not a function catalogue or a host memory grant.

use crate::builtin::window_default_numeric::NumericDefaultRecipe;
use crate::kernel_control::{compile_failure, internal, invalid};
use crate::kernel_input::{
    EvaluationCheckpoints, validate_argument_observed, validate_type_observed,
};
use crate::{EvaluatedArgument, KernelEvaluationControl, KernelFailure, Selection};
use arrow_array::{
    Array, ArrayRef, Int8Array, Int16Array, Int32Array, Int64Array, NullArray, StringArray,
    UInt64Array,
};
use arrow_schema::DataType;
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, FunctionValueType, PureCompileControl, ValueLogicalType,
};
use std::{alloc::Layout, ops::Range};

#[derive(Debug)]
enum Conversion {
    Identity,
    Numeric(NumericDefaultRecipe),
    Parse,
    Format,
    Null,
}

#[derive(Debug)]
pub(super) struct DefaultValueRecipe {
    source: FunctionValueType,
    target: FunctionValueType,
    conversion: Conversion,
}

fn numeric(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
    )
}

#[derive(Clone, Copy)]
enum DefaultProfile {
    Numeric,
    Identity,
    Null,
    Parse,
    Format,
}
#[derive(Clone, Copy)]
enum UnsupportedDefaultProfile {
    Target,
    Numeric,
    Conversion,
}
impl UnsupportedDefaultProfile {
    fn failure(self) -> KernelFailure {
        invalid(match self {
            Self::Target => "window default requires an implemented nullable result domain",
            Self::Numeric => {
                "window numeric default requires Physical carriers and a nullable result"
            }
            Self::Conversion => "window default conversion profile is unsupported",
        })
    }
}
fn default_profile(
    source: &FunctionValueType,
    target: &FunctionValueType,
) -> Result<DefaultProfile, UnsupportedDefaultProfile> {
    if !target.nullable() || !(numeric(target.data_type()) || target.data_type() == &DataType::Utf8)
    {
        return Err(UnsupportedDefaultProfile::Target);
    }
    if let Some(supported) = NumericDefaultRecipe::profile_supported(source, target) {
        return if supported {
            Ok(DefaultProfile::Numeric)
        } else {
            Err(UnsupportedDefaultProfile::Numeric)
        };
    }
    let physical = source.logical_type() == ValueLogicalType::Physical
        && target.logical_type() == ValueLogicalType::Physical;
    if source.data_type() == target.data_type() && source.logical_type() == target.logical_type() {
        Ok(DefaultProfile::Identity)
    } else if source.data_type() == &DataType::Null
        && source.logical_type() == ValueLogicalType::Physical
    {
        Ok(DefaultProfile::Null)
    } else if physical
        && source.data_type() == &DataType::Utf8
        && target.data_type() == &DataType::Int64
    {
        Ok(DefaultProfile::Parse)
    } else if physical
        && source.data_type() == &DataType::Int64
        && target.data_type() == &DataType::Utf8
    {
        Ok(DefaultProfile::Format)
    } else {
        Err(UnsupportedDefaultProfile::Conversion)
    }
}

impl DefaultValueRecipe {
    /// The exact type-only decision also used by the original preparation body.
    /// It does not inspect defaults, offsets, frames or partition payload.
    pub(super) fn supports_profile(source: &FunctionValueType, target: &FunctionValueType) -> bool {
        default_profile(source, target).is_ok()
    }

    pub(super) fn try_new(
        source: &FunctionValueType,
        target: &FunctionValueType,
        control: &dyn PureCompileControl,
    ) -> Result<Self, KernelFailure> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(compile_failure)?;
        let result = (|| {
            validate_type_observed(source, &mut work)?;
            validate_type_observed(target, &mut work)?;
            let profile = default_profile(source, target);
            work.step().map_err(compile_failure)?;
            let conversion = match profile.map_err(UnsupportedDefaultProfile::failure)? {
                DefaultProfile::Numeric => {
                    let recipe = NumericDefaultRecipe::try_new(source, target, &mut work)?
                        .ok_or_else(|| {
                            internal("numeric window default lost its admitted profile")
                        })?;
                    Conversion::Numeric(recipe)
                }
                DefaultProfile::Identity => Conversion::Identity,
                DefaultProfile::Null => Conversion::Null,
                DefaultProfile::Parse => Conversion::Parse,
                DefaultProfile::Format => Conversion::Format,
            };
            work.step().map_err(compile_failure)?;
            // Only admitted flat carriers reach these clones. No nested Field,
            // metadata, Box graph, authority, control or new policy is retained.
            let recipe = Self {
                source: source.clone(),
                target: target.clone(),
                conversion,
            };
            work.step().map_err(compile_failure)?;
            Ok(recipe)
        })();
        if result.as_ref().is_err_and(KernelFailure::is_interruption) {
            return result;
        }
        work.finish().map_err(compile_failure)?;
        result
    }
    #[cfg(test)]
    pub(super) fn source_type(&self) -> &FunctionValueType {
        &self.source
    }
    #[cfg(test)]
    pub(super) fn result_type(&self) -> &FunctionValueType {
        &self.target
    }

    /// Upper bound for the returned array's `get_array_memory_size()` metric,
    /// not an allocation grant or a bound on temporary cast/take coexistence,
    /// Arc headers, private Bytes owners, or installed host accounting.
    pub(super) fn retained_upper_bound(&self, rows: usize) -> Result<usize, KernelFailure> {
        if let Conversion::Numeric(recipe) = &self.conversion {
            return recipe.retained_upper_bound(rows);
        }
        if numeric(self.target.data_type()) {
            return NumericDefaultRecipe::retained_target_upper_bound(
                self.target.data_type(),
                rows,
            );
        }
        let bitmap = round64(
            rows.checked_add(7)
                .ok_or(KernelFailure::ResourceExhausted)?
                / 8,
        )?;
        let (header, values, offsets, nulls) = match self.target.data_type() {
            DataType::Utf8 => {
                let offsets = rows
                    .checked_add(1)
                    .ok_or(KernelFailure::ResourceExhausted)?;
                match self.conversion {
                    Conversion::Format => {
                        // The actual writer checks measured bytes <= i32::MAX.
                        // Its row-independent output upper may use that global
                        // bound without rejecting long arrays of small numbers.
                        let bytes = mul(rows, 20)?.min(i32::MAX as usize);
                        let (o, v, n) = formatter_capacities(rows, bytes)?;
                        (std::mem::size_of::<StringArray>(), v, o, n)
                    }
                    Conversion::Null => (
                        std::mem::size_of::<StringArray>(),
                        0,
                        round64(mul(offsets, 4)?)?,
                        bitmap,
                    ),
                    Conversion::Identity => (
                        std::mem::size_of::<StringArray>(),
                        if rows == 0 { 0 } else { i32::MAX as usize },
                        mul(offsets, 4)?,
                        bitmap,
                    ),
                    _ => return Err(internal("window default retained output profile differs")),
                }
            }
            _ => return Err(internal("window default retained output carrier differs")),
        };
        let total = [header, values, offsets, nulls]
            .into_iter()
            .try_fold(0usize, |n, x| {
                n.checked_add(x).ok_or(KernelFailure::ResourceExhausted)
            })?;
        // Distinct buffers need distinct legal Layouts. Their cumulative
        // retained metric is not one allocation (including on 32-bit targets).
        for bytes in [header, values, offsets, nulls] {
            Layout::array::<u8>(bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
        }
        Ok(total)
    }

    pub(super) fn evaluate_complete(
        &self,
        input: EvaluatedArgument<'_>,
        rows: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure> {
        control.checkpoint(0)?;
        // The shared validator owns its complete scope. Return every original
        // refusal directly; no local pending tail may recheck a nested cause.
        validate_argument_observed(input, Selection::all(rows), &self.source, control)?;
        let mut work = EvaluationCheckpoints::new(control);
        let result = (|| {
            let class = if let Conversion::Numeric(recipe) = &self.conversion {
                recipe.source_class(input.array().as_ref())
            } else {
                match self.source.data_type() {
                    DataType::Int8 => input.array().as_any().is::<Int8Array>(),
                    DataType::Int16 => input.array().as_any().is::<Int16Array>(),
                    DataType::Int32 => input.array().as_any().is::<Int32Array>(),
                    DataType::Int64 => input.array().as_any().is::<Int64Array>(),
                    DataType::Utf8 => input.array().as_any().is::<StringArray>(),
                    DataType::Null => input.array().as_any().is::<NullArray>(),
                    _ => false,
                }
            };
            work.step()?;
            if !class {
                return Err(invalid("window default source has a foreign carrier class"));
            }
            gather_layout(rows)?;
            if let Conversion::Numeric(recipe) = &self.conversion {
                recipe.preflight_output(rows)?;
            } else {
                output_layout(self.target.data_type(), rows, 0)?;
            }
            if matches!(self.conversion, Conversion::Format) {
                let array = input
                    .array()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| invalid("window default I64 formatter has a foreign carrier"))?;
                let mut bytes = 0usize;
                for row in 0..rows {
                    let at = input.value_row(row, row);
                    if !array.is_null(at) {
                        bytes = bytes
                            .checked_add(decimal_bytes(array.value(at), &mut work)?)
                            .ok_or(KernelFailure::ResourceExhausted)?;
                    }
                    work.step()?;
                }
                output_layout(self.target.data_type(), rows, bytes)?;
                formatter_capacities(rows, bytes)?;
            }
            // Gather only complete logical D rows. Scalar and selected CV values
            // never cast unrelated physical pool ordinals or hidden backing rows.
            work.flush()?;
            let mut addresses = Vec::new();
            addresses
                .try_reserve_exact(rows)
                .map_err(|_| KernelFailure::ResourceExhausted)?;
            work.flush()?;
            for row in 0..rows {
                addresses.push(Some(
                    u64::try_from(input.value_row(row, row))
                        .map_err(|_| KernelFailure::ResourceExhausted)?,
                ));
                work.step()?;
            }
            work.flush()?;
            copy_result(
                crate::selected_copy::preflight_take(
                    input.array().as_ref(),
                    &addresses,
                    |opaque| {
                        if opaque { work.flush() } else { work.step() }
                    },
                ),
                &mut work,
            )?;
            work.flush()?;
            let indices = UInt64Array::from(addresses);
            work.flush()?;
            let gathered = arrow_select::take::take(input.array().as_ref(), &indices, None);
            work.flush()?;
            let gathered =
                gathered.map_err(|_| internal("window default complete logical gather failed"))?;
            // Raw parsing retains the original strict text domain. Library
            // parse/formatter work is opaque, with original before/after checks;
            // no fictional byte prescan claims internal 256-unit cooperation.
            work.flush()?;
            let converted = if matches!(self.conversion, Conversion::Parse) {
                let text = gathered
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .ok_or_else(|| internal("window default parse has a foreign text carrier"))?;
                crate::math_numeric::cast_raw_numeric(text, self.target.data_type())
                    .map_err(|_| invalid("window default safe Arrow conversion failed"))
            } else {
                arrow_cast::cast(gathered.as_ref(), self.target.data_type())
                    .map_err(|_| invalid("window default safe Arrow conversion failed"))
            };
            work.flush()?;
            let converted = converted?;
            work.step()?;
            Ok(converted)
        })();
        if result.as_ref().is_err_and(KernelFailure::is_interruption) {
            return result;
        }
        work.finish()?;
        let converted = result?;
        // Shared final output validation has its own ordinary/control exit.
        // There is no caller checkpoint after a refusal from this scope.
        validate_argument_observed(
            EvaluatedArgument::Column(&converted),
            Selection::all(rows),
            &self.target,
            control,
        )?;
        Ok(converted)
    }
}

fn mul(left: usize, right: usize) -> Result<usize, KernelFailure> {
    left.checked_mul(right)
        .ok_or(KernelFailure::ResourceExhausted)
}
fn round64(bytes: usize) -> Result<usize, KernelFailure> {
    let rounded = bytes
        .checked_add(63)
        .ok_or(KernelFailure::ResourceExhausted)?
        & !63;
    Layout::array::<u8>(rounded).map_err(|_| KernelFailure::ResourceExhausted)?;
    Ok(rounded)
}
fn formatter_capacities(rows: usize, bytes: usize) -> Result<(usize, usize, usize), KernelFailure> {
    // Arrow60.1.0 GenericStringBuilder::new has 1024 items/data bytes,
    // hence 1025 offsets. Rust1.98.1 RawVec and MutableBuffer doubling are
    // bounded by twice the larger of initial capacity and final extent.
    let offsets = mul(
        mul(
            rows.checked_add(1)
                .ok_or(KernelFailure::ResourceExhausted)?
                .max(1025),
            2,
        )?,
        4,
    )?;
    let values = mul(bytes.max(1024), 2)?;
    let bitmap = mul(
        round64(
            rows.max(1024)
                .checked_add(7)
                .ok_or(KernelFailure::ResourceExhausted)?
                / 8,
        )?,
        2,
    )?;
    for bytes in [offsets, values, bitmap] {
        Layout::array::<u8>(bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
    }
    Ok((offsets, values, bitmap))
}
fn gather_layout(rows: usize) -> Result<(), KernelFailure> {
    Layout::array::<Option<u64>>(rows).map_err(|_| KernelFailure::ResourceExhausted)?;
    Layout::array::<Range<usize>>(rows).map_err(|_| KernelFailure::ResourceExhausted)?;
    Layout::array::<u64>(rows).map_err(|_| KernelFailure::ResourceExhausted)?;
    let bitmap = rows
        .checked_add(7)
        .ok_or(KernelFailure::ResourceExhausted)?
        / 8;
    Layout::array::<u8>(bitmap).map_err(|_| KernelFailure::ResourceExhausted)?;
    Ok(())
}
fn output_layout(ty: &DataType, rows: usize, bytes: usize) -> Result<(), KernelFailure> {
    if numeric(ty) {
        return NumericDefaultRecipe::preflight_target(ty, rows);
    }
    let bitmap = rows
        .checked_add(7)
        .ok_or(KernelFailure::ResourceExhausted)?
        / 8;
    Layout::array::<u8>(bitmap).map_err(|_| KernelFailure::ResourceExhausted)?;
    match ty {
        DataType::Utf8 => {
            i32::try_from(bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
            Layout::array::<u8>(bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
            Layout::array::<i32>(
                rows.checked_add(1)
                    .ok_or(KernelFailure::ResourceExhausted)?,
            )
        }
        _ => {
            return Err(invalid(
                "window default output representation is unsupported",
            ));
        }
    }
    .map_err(|_| KernelFailure::ResourceExhausted)?;
    Ok(())
}
fn decimal_bytes(value: i64, work: &mut EvaluationCheckpoints<'_>) -> Result<usize, KernelFailure> {
    // Decimal display of an i64 uses at most 19 digits plus its minus sign.
    // This measures representation only; Arrow's sole formatter writes bytes.
    let mut magnitude = value.unsigned_abs();
    let mut count = 1 + usize::from(value < 0);
    work.step()?;
    while magnitude >= 10 {
        magnitude /= 10;
        count += 1;
        work.step()?;
    }
    Ok(count)
}
fn copy_result(
    result: Result<(), crate::selected_copy::CopyError>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    match result {
        Ok(()) => work.flush(),
        Err(source) => Err(source.into_kernel()),
    }
}

#[cfg(test)]
#[path = "window_default_tests.rs"]
mod tests;
