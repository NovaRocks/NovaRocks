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

//! Selected numeric kernels for the typed ROUND and TRUNCATE calls.

use crate::math_numeric::NumericArrayView;
use crate::{
    DataFailureReason, EvaluatedArgument, KernelEvaluationControl, KernelFailure, RowDataError,
    ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{
    Array, Int32Array, PrimitiveArray, builder::PrimitiveBuilder, types::ArrowPrimitiveType,
};
use arrow_schema::DataType;
use std::sync::Arc;

fn round_float(value: f64, digits: i32) -> f64 {
    if !value.is_finite() {
        return value;
    }
    let factor = 10_f64.powf(f64::from(digits));
    let sign = if value < 0.0 { -1.0 } else { 1.0 };
    if value == 0.0 || factor == 0.0 {
        return sign * 0.0;
    }
    let rescaled = sign * value * factor;
    if rescaled.is_infinite() {
        return value;
    }
    sign * (rescaled.round() / factor)
}

fn round_integer(value: i64, digits: i32) -> Option<i64> {
    if digits >= 0 {
        return Some(value);
    }
    let factor = 10_i64.checked_pow(digits.unsigned_abs())?;
    let quotient = value / factor;
    let remainder = value % factor;
    let adjust = if remainder.unsigned_abs() >= factor as u64 / 2 {
        value.signum()
    } else {
        0
    };
    (quotient + adjust).checked_mul(factor)
}

fn decimal_at_digits(
    value: i128,
    source_scale: i8,
    output_precision: u8,
    digits: Option<i32>,
    round: bool,
) -> Option<i128> {
    let removed = i64::from(source_scale) - i64::from(digits.unwrap_or(0));
    if removed <= 0 {
        return Some(value);
    }
    if removed > 38 {
        // The long decimal ROUND path rescales the rounded coefficient back
        // with an Int128 power of ten. Its largest representable power is 38.
        return if round && output_precision > 18 {
            None
        } else {
            Some(0)
        };
    }
    let factor = 10_i128.pow(removed as u32);
    let quotient = value / factor;
    let remainder = value % factor;
    let adjusted = if round && remainder.unsigned_abs() >= factor as u128 / 2 {
        quotient + value.signum()
    } else {
        quotient
    };
    if digits.is_some() {
        adjusted.checked_mul(factor)
    } else {
        Some(adjusted)
    }
}

#[inline]
fn selected_row(
    argument: EvaluatedArgument<'_>,
    ordinal: usize,
    row: usize,
) -> Result<Option<usize>, KernelFailure> {
    let row = argument.value_row(ordinal, row);
    let array = argument.array();
    if row >= array.len() {
        return Err(internal("rounding selected row is outside its carrier"));
    }
    Ok(array.is_valid(row).then_some(row))
}

/// The checked call and selected primitive storage determine the exact kernel.
pub(super) fn evaluate_rounding<'a>(
    round: bool,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let (value, digits) = match input.arguments() {
        [value] => (*value, None),
        [value, digits] => {
            let array = digits
                .array()
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| invalid("rounding digits must have the INTEGER carrier"))?;
            (*value, Some((*digits, array)))
        }
        _ => return Err(invalid("rounding call has incorrect arity")),
    };
    let target = input.contract().result_type().data_type();
    let view = NumericArrayView::new(value.array()).map_err(KernelFailure::from_domain)?;
    match (view, target) {
        (NumericArrayView::Int8(array), DataType::Int8) if round => {
            evaluate_fixed(array, value, digits, &input, control, |v, d| {
                i8::try_from(round_integer(i64::from(v), d.unwrap_or(0))?).ok()
            })
        }
        (NumericArrayView::Int16(array), DataType::Int16) if round => {
            evaluate_fixed(array, value, digits, &input, control, |v, d| {
                i16::try_from(round_integer(i64::from(v), d.unwrap_or(0))?).ok()
            })
        }
        (NumericArrayView::Int32(array), DataType::Int32) if round => {
            evaluate_fixed(array, value, digits, &input, control, |v, d| {
                i32::try_from(round_integer(i64::from(v), d.unwrap_or(0))?).ok()
            })
        }
        (NumericArrayView::Int64(array), DataType::Int64) if round => {
            evaluate_fixed(array, value, digits, &input, control, |v, d| {
                round_integer(v, d.unwrap_or(0))
            })
        }
        (NumericArrayView::Float32(array), DataType::Float32) => {
            evaluate_fixed(array, value, digits, &input, control, |v, d| {
                Some(if round {
                    if v.is_finite() {
                        round_float(f64::from(v), d.unwrap_or(0)) as f32
                    } else {
                        v
                    }
                } else {
                    v.trunc()
                })
            })
        }
        (NumericArrayView::Float64(array), DataType::Float64) => {
            evaluate_fixed(array, value, digits, &input, control, |v, d| {
                Some(if round {
                    round_float(v, d.unwrap_or(0))
                } else {
                    v.trunc()
                })
            })
        }
        (NumericArrayView::Decimal128(array, scale), DataType::Decimal128(precision, _)) => {
            let limit = 10_i128.pow(u32::from(*precision)) as u128;
            evaluate_fixed(array, value, digits, &input, control, |v, d| {
                decimal_at_digits(v, scale, *precision, d, round)
                    .filter(|result| result.unsigned_abs() < limit)
            })
        }
        _ => Err(invalid(
            "rounding source and result storage differ from the exact signature",
        )),
    }
}

fn evaluate_fixed<'a, T: ArrowPrimitiveType>(
    array: &PrimitiveArray<T>,
    value: EvaluatedArgument<'a>,
    digits: Option<(EvaluatedArgument<'a>, &Int32Array)>,
    input: &ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
    operation: impl Fn(T::Native, Option<i32>) -> Option<T::Native>,
) -> Result<SelectedValues<'a>, KernelFailure> {
    let selection = input.selection();
    let target = input.contract().result_type().data_type();
    for bytes in [
        std::mem::size_of::<T::Native>(),
        std::mem::size_of::<RowDataError>() + crate::MAX_ROW_ERROR_MESSAGE_BYTES,
    ] {
        std::alloc::Layout::array::<u8>(
            selection
                .len()
                .checked_mul(bytes)
                .ok_or(KernelFailure::ResourceExhausted)?,
        )
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    }
    let mut output =
        PrimitiveBuilder::<T>::with_capacity(selection.len()).with_data_type(target.clone());
    let mut errors = Vec::new();
    let mut work = EvaluationCheckpoints::new(control);
    for (ordinal, row) in selection.iter().enumerate() {
        work.step()?;
        let Some(value_row) = selected_row(value, ordinal, row)? else {
            output.append_null();
            continue;
        };
        let digit = if let Some((digits, array)) = digits {
            let Some(digit_row) = selected_row(digits, ordinal, row)? else {
                output.append_null();
                continue;
            };
            Some(array.value(digit_row))
        } else {
            None
        };
        let result = operation(array.value(value_row), digit);
        if result.is_none() {
            errors.push(RowDataError::with_reason(
                ordinal,
                DataFailureReason::NumericOutOfRange,
                "numeric overflow in rounding",
            ));
        }
        output.append_option(result);
    }
    work.finish()?;
    SelectedValues::try_new(
        selection,
        target,
        Arc::new(output.finish()),
        errors.into_boxed_slice(),
    )
    .map_err(|_| internal("rounding output violates its selected contract"))
}

#[cfg(test)]
pub(super) mod tests;
