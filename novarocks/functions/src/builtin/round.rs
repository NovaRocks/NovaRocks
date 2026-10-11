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

//! Selected ROUND computations with exact, prepared Arrow scalar cast paths.

use super::{
    binding_control,
    round_cast::{CastRecipe, CastValue},
    round_core::round_float_to_integer as round_float_integer,
    rounding_binding::CastTarget,
};
use crate::{
    FunctionArgumentType, FunctionBindingError, FunctionBindingSelection, FunctionResultType,
    FunctionSpecializationFailure, KernelEvaluationControl, KernelFailure, RowDataError,
    ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{
    Array, ArrayRef, Decimal128Array,
    builder::{Decimal128Builder, Float64Builder, Int64Builder},
    types::{Decimal128Type, DecimalType},
};
use arrow_schema::DataType;
use novarocks_type_contract::{DecimalOverflowPolicy, PureCompileControl, ValueLogicalType};
use std::sync::Arc;

#[derive(Clone, Copy, Debug)]
enum ValueRecipe {
    Float(CastRecipe),
    Decimal { source_scale: i8, output_scale: i8 },
}
#[derive(Clone, Copy, Debug)]
pub(super) struct RoundRecipe {
    value: ValueRecipe,
    digits: Option<CastRecipe>,
}
impl RoundRecipe {
    pub(super) fn prepare(
        selected: &FunctionBindingSelection,
        control: &dyn PureCompileControl,
    ) -> Result<Self, FunctionSpecializationFailure> {
        binding_control::scope(control, |work| {
            for argument in &selected.argument_types {
                work.step()?;
                let FunctionArgumentType::Value(source) = argument else {
                    return Err(FunctionBindingError::NoMatchingOverload);
                };
                binding_control::value_type(source, work)?;
            }
            let FunctionResultType::Scalar(target) = &selected.result_type else {
                return Err(FunctionBindingError::NoMatchingOverload);
            };
            binding_control::value_type(target, work)?;
            let sources = match selected.argument_types.as_ref() {
                [FunctionArgumentType::Value(source)] => (source, None),
                [
                    FunctionArgumentType::Value(source),
                    FunctionArgumentType::Value(digits),
                ] => (source, Some(digits)),
                _ => return Err(FunctionBindingError::NoMatchingOverload),
            };
            if target.logical_type != ValueLogicalType::Physical || !target.nullable {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            let value = match sources.0.data_type {
                DataType::Decimal128(_, source_scale) => {
                    let DataType::Decimal128(38, output_scale) = target.data_type else {
                        return Err(FunctionBindingError::NoMatchingOverload);
                    };
                    ValueRecipe::Decimal {
                        source_scale,
                        output_scale,
                    }
                }
                _ => {
                    let valid = if sources.1.is_some() {
                        target.data_type == DataType::Float64
                    } else {
                        target.data_type == DataType::Int64
                    };
                    if !valid {
                        return Err(FunctionBindingError::NoMatchingOverload);
                    }
                    ValueRecipe::Float(CastRecipe::prepare(sources.0, CastTarget::Float64, work)?)
                }
            };
            let digits = sources
                .1
                .map(|digits| CastRecipe::prepare(digits, CastTarget::Int64, work))
                .transpose()?;
            work.step()?;
            Ok(Self { value, digits })
        })
        .map_err(Into::into)
    }
    fn arity(&self) -> usize {
        if self.digits.is_some() { 2 } else { 1 }
    }
    fn check_available(&self) -> Result<(), KernelFailure> {
        if let ValueRecipe::Float(recipe) = &self.value {
            recipe.check_available()?;
        }
        if let Some(recipe) = &self.digits {
            recipe.check_available()?;
        }
        Ok(())
    }
}

fn round_float_digits(value: f64, digits: i64) -> f64 {
    let negative = digits < 0;
    let magnitude = digits.unsigned_abs();
    let factor = if magnitude < 10 {
        let mut value = 1.0;
        for _ in 0..magnitude {
            value *= 10.0;
        }
        value
    } else {
        10_f64.powi(magnitude as i32)
    };
    let divided = value / factor;
    let multiplied = value * factor;
    if negative && factor.is_infinite() {
        return 0.0;
    }
    if !negative && multiplied.is_infinite() {
        return value;
    }
    if negative {
        divided.round() * factor
    } else {
        multiplied.round() / factor
    }
}
fn decimal_rescale(value: i128, source: i8, target: i8) -> Option<i128> {
    let difference = i32::from(target) - i32::from(source);
    if difference == 0 {
        return Some(value);
    }
    let factor = 10_i128.checked_pow(difference.unsigned_abs())?;
    if difference > 0 {
        return value.checked_mul(factor);
    }
    let quotient = value.checked_div(factor)?;
    let remainder = value.checked_rem(factor)?;
    // Powers above one are even. Compare magnitude to half without a doubled
    // remainder or value+half intermediate, including valid precision-38 data.
    if remainder.unsigned_abs() >= factor.unsigned_abs() / 2 {
        quotient.checked_add(if value < 0 { -1 } else { 1 })
    } else {
        Some(quotient)
    }
}
fn rounded_decimal(value: i128, source: i8, output: i8, digits: Option<i64>) -> Option<i128> {
    let rounded = match digits {
        None => decimal_rescale(value, source, output)?,
        Some(digits) => {
            let requested = digits.clamp(-38, 38) as i8;
            let rounded = decimal_rescale(value, source, requested)?;
            // The installed adjustment rounds again when lowering to the
            // frozen scale. Literal scale projection can differ from the
            // clamped runtime digits, so this is not a truncating division.
            decimal_rescale(rounded, requested, output)?
        }
    };
    Decimal128Type::is_valid_decimal_precision(rounded, 38).then_some(rounded)
}

enum Output {
    Integer(Int64Builder),
    Float(Float64Builder),
    Decimal(Decimal128Builder),
}
impl Output {
    fn new(recipe: &RoundRecipe, rows: usize) -> Result<Self, KernelFailure> {
        Ok(match &recipe.value {
            ValueRecipe::Float(_) if recipe.digits.is_none() => {
                Self::Integer(Int64Builder::with_capacity(rows))
            }
            ValueRecipe::Float(_) => Self::Float(Float64Builder::with_capacity(rows)),
            ValueRecipe::Decimal { output_scale, .. } => Self::Decimal(
                Decimal128Builder::with_capacity(rows)
                    .with_precision_and_scale(38, *output_scale)
                    .map_err(|_| invalid("round frozen Decimal output parameters are invalid"))?,
            ),
        })
    }
    fn append_null(&mut self) {
        match self {
            Self::Integer(b) => b.append_null(),
            Self::Float(b) => b.append_null(),
            Self::Decimal(b) => b.append_null(),
        }
    }
    fn append_float(&mut self, value: f64, digits: Option<i64>) -> Result<(), KernelFailure> {
        match (self, digits) {
            (Self::Integer(builder), None) => builder.append_value(round_float_integer(value)),
            (Self::Float(builder), Some(digits)) => {
                builder.append_value(round_float_digits(value, digits))
            }
            _ => {
                return Err(internal(
                    "round output differs from the frozen floating operation",
                ));
            }
        }
        Ok(())
    }
    fn append_decimal(&mut self, value: Option<i128>) -> Result<(), KernelFailure> {
        let Self::Decimal(builder) = self else {
            return Err(internal("round output differs from its decimal operation"));
        };
        builder.append_option(value);
        Ok(())
    }
    fn finish(self) -> ArrayRef {
        match self {
            Self::Integer(mut b) => Arc::new(b.finish()),
            Self::Float(mut b) => Arc::new(b.finish()),
            Self::Decimal(mut b) => Arc::new(b.finish()),
        }
    }
}
fn row_overflow(
    policy: DecimalOverflowPolicy,
    ordinal: usize,
    errors: &mut Vec<RowDataError>,
    message: &str,
) {
    if policy == DecimalOverflowPolicy::ReportError {
        errors.push(RowDataError::new(ordinal, message));
    }
}
fn capacity(rows: usize) -> Result<(), KernelFailure> {
    for width in [
        16,
        std::mem::size_of::<RowDataError>() + crate::MAX_ROW_ERROR_MESSAGE_BYTES,
    ] {
        let bytes = rows
            .checked_mul(width)
            .ok_or(KernelFailure::ResourceExhausted)?;
        isize::try_from(bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
    }
    rows.checked_add(7)
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok(())
}

pub(super) fn evaluate_round<'a>(
    recipe: &RoundRecipe,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    if input.arguments().len() != recipe.arity() {
        return Err(invalid("round arguments differ from the frozen arity"));
    }
    let value = input.arguments()[0];
    let digits = input.arguments().get(1);
    let decimal = match &recipe.value {
        ValueRecipe::Decimal { .. } => Some(
            value
                .array()
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| internal("round decimal input cannot be downcast"))?,
        ),
        ValueRecipe::Float(_) => None,
    };
    // Deferred static caster failures remain outside the maskable row channel.
    // Empty/unreachable calls are skipped by ScalarEvaluationInstance/controller.
    recipe.check_available()?;
    let selection = input.selection();
    capacity(selection.len())?;
    let mut output = Output::new(recipe, selection.len())?;
    let mut errors = Vec::new();
    let policy = input.contract().decimal_overflow_policy();
    let mut work = EvaluationCheckpoints::new(control);
    for (ordinal, batch_row) in selection.iter().enumerate() {
        work.step()?;
        let value_row = value.value_row(ordinal, batch_row);
        let floating = match &recipe.value {
            ValueRecipe::Float(recipe) => {
                match recipe.read(value.array().as_ref(), value_row, &mut work)? {
                    CastValue::Float(value) => Some(value),
                    CastValue::Null => {
                        output.append_null();
                        continue;
                    }
                    _ => {
                        return Err(internal(
                            "round frozen Float64 caster returned a wrong kind",
                        ));
                    }
                }
            }
            ValueRecipe::Decimal { .. } => {
                let array = decimal.ok_or_else(|| internal("round decimal reader is absent"))?;
                if value_row >= array.len() {
                    return Err(internal(
                        "round decimal selected row is outside its carrier",
                    ));
                }
                if array.is_null(value_row) {
                    output.append_null();
                    continue;
                }
                None
            }
        };
        let digits = match (&recipe.digits, digits) {
            (None, None) => None,
            (Some(recipe), Some(argument)) => {
                let row = argument.value_row(ordinal, batch_row);
                match recipe.read(argument.array().as_ref(), row, &mut work)? {
                    CastValue::Integer(digits) => Some(digits),
                    CastValue::Null => {
                        output.append_null();
                        continue;
                    }
                    CastValue::CheckedDecimalOverflow => {
                        row_overflow(
                            policy,
                            ordinal,
                            &mut errors,
                            "decimal overflow in round digits",
                        );
                        output.append_null();
                        continue;
                    }
                    _ => return Err(internal("round frozen Int64 caster returned a wrong kind")),
                }
            }
            _ => return Err(invalid("round digits differ from the frozen recipe")),
        };
        match (&recipe.value, floating) {
            (ValueRecipe::Float(_), Some(value)) => output.append_float(value, digits)?,
            (
                ValueRecipe::Decimal {
                    source_scale,
                    output_scale,
                },
                None,
            ) => {
                let raw = decimal
                    .ok_or_else(|| internal("round decimal reader is absent"))?
                    .value(value_row);
                let rounded = rounded_decimal(raw, *source_scale, *output_scale, digits);
                if rounded.is_none() {
                    row_overflow(
                        policy,
                        ordinal,
                        &mut errors,
                        "decimal overflow in round output",
                    );
                }
                output.append_decimal(rounded)?;
            }
            _ => return Err(internal("round frozen value recipe returned a wrong kind")),
        }
    }
    work.finish()?;
    // Capacity and retained-size declarations are representation bounds only.
    // The production host still owes formal memory authorization before builders.
    SelectedValues::try_new(
        selection,
        &input.contract().result_type().data_type,
        output.finish(),
        errors.into_boxed_slice(),
    )
    .map_err(|_| internal("round compact output violates its selected contract"))
}

#[cfg(test)]
#[path = "round_tests.rs"]
mod tests;
