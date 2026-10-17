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

//! Admission of SQL syntax values into the sole checked constant owner.
//! Materialized values must retain their original owner instead of entering
//! this syntax author. Construction preflight is not a formal host MEM grant.

use crate::common::LiteralValue;
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{DataType, i256};
use novarocks_constant_contract::{
    ConstantError, ConstantPolicy, ConstantValue, preflight_scalar_construction,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, FunctionValueType, PureCompileControl,
};
use std::sync::Arc;

pub(crate) fn admit_syntax_constant(
    value: &LiteralValue,
    value_type: &FunctionValueType,
    policy: ConstantPolicy,
    control: &dyn PureCompileControl,
) -> Result<ConstantValue, ConstantError> {
    let phase = CompilePhase::FunctionSpecialization;
    let mut work = CompileCheckpoints::try_new(control, phase)?;
    let result = (|| {
        novarocks_functions::validate_function_value_type_observed(value_type, &mut work).map_err(
            |error| match error {
                novarocks_functions::KernelFailure::Cancelled => {
                    ConstantError::Control(novarocks_type_contract::CompileControlError::Cancelled)
                }
                novarocks_functions::KernelFailure::DeadlineExceeded => ConstantError::Control(
                    novarocks_type_contract::CompileControlError::DeadlineExceeded,
                ),
                novarocks_functions::KernelFailure::ResourceExhausted => ConstantError::Control(
                    novarocks_type_contract::CompileControlError::ResourceExhausted,
                ),
                error => ConstantError::Arrow(error.to_string()),
            },
        )?;
        let field = Arc::new(arrow::datatypes::Field::new(
            "literal",
            value_type.data_type().clone(),
            value_type.nullable(),
        ));
        work.flush()?;
        let payload = match value {
            LiteralValue::String(value) => value.len() as u64,
            LiteralValue::Binary(value) => value.len() as u64,
            _ => 0,
        };
        preflight_scalar_construction(
            &field,
            value_type,
            payload,
            matches!(value, LiteralValue::Null),
            policy,
            phase,
            work.control(),
        )?;
        work.flush()?;
        macro_rules! scalar {
            ($factory:ident, $value:expr) => {
                ConstantValue::$factory(
                    field,
                    value_type.clone(),
                    $value,
                    policy,
                    phase,
                    work.control(),
                )
            };
        }
        let checked = match value {
            LiteralValue::Null => {
                ConstantValue::null(field, value_type.clone(), policy, phase, work.control())
            }
            LiteralValue::Bool(value) => scalar!(from_boolean, *value),
            LiteralValue::LargeInt(value) => scalar!(from_largeint, *value),
            LiteralValue::String(value) => scalar!(from_utf8, value),
            LiteralValue::Binary(value) => scalar!(from_binary, value),
            LiteralValue::Int(value) => {
                macro_rules! signed {
                    ($array:ty, $native:ty) => {{
                        let value = <$native>::try_from(*value).map_err(|_| {
                            ConstantError::Invalid(
                                "SQL integer syntax exceeds its declared carrier",
                            )
                        })?;
                        Arc::new(<$array>::from(vec![value])) as ArrayRef
                    }};
                }
                let array: ArrayRef = match value_type.data_type() {
                    DataType::Int8 => signed!(arrow::array::Int8Array, i8),
                    DataType::Int16 => signed!(arrow::array::Int16Array, i16),
                    DataType::Int32 => signed!(arrow::array::Int32Array, i32),
                    DataType::Int64 => signed!(arrow::array::Int64Array, i64),
                    DataType::UInt8 => signed!(arrow::array::UInt8Array, u8),
                    DataType::UInt16 => signed!(arrow::array::UInt16Array, u16),
                    DataType::UInt32 => signed!(arrow::array::UInt32Array, u32),
                    DataType::UInt64 => signed!(arrow::array::UInt64Array, u64),
                    DataType::Date32 => signed!(arrow::array::Date32Array, i32),
                    DataType::Date64 => signed!(arrow::array::Date64Array, i64),
                    DataType::Time64(arrow::datatypes::TimeUnit::Microsecond) => {
                        signed!(arrow::array::Time64MicrosecondArray, i64)
                    }
                    DataType::Time64(arrow::datatypes::TimeUnit::Nanosecond) => {
                        signed!(arrow::array::Time64NanosecondArray, i64)
                    }
                    DataType::Timestamp(unit, zone) => {
                        macro_rules! timestamp {
                            ($array:ty) => {
                                Arc::new(
                                    <$array>::from(vec![*value]).with_timezone_opt(zone.clone()),
                                ) as ArrayRef
                            };
                        }
                        match unit {
                            arrow::datatypes::TimeUnit::Second => {
                                timestamp!(arrow::array::TimestampSecondArray)
                            }
                            arrow::datatypes::TimeUnit::Millisecond => {
                                timestamp!(arrow::array::TimestampMillisecondArray)
                            }
                            arrow::datatypes::TimeUnit::Microsecond => {
                                timestamp!(arrow::array::TimestampMicrosecondArray)
                            }
                            arrow::datatypes::TimeUnit::Nanosecond => {
                                timestamp!(arrow::array::TimestampNanosecondArray)
                            }
                        }
                    }
                    _ => {
                        return Err(ConstantError::Invalid(
                            "SQL integer syntax requires its exact declared integer/date carrier",
                        ));
                    }
                };
                work.flush()?;
                ConstantValue::from_scalar_array(
                    field,
                    value_type.clone(),
                    array.to_data(),
                    policy,
                    phase,
                    work.control(),
                )
            }
            LiteralValue::Float(value) => match value_type.data_type() {
                DataType::Float64 => scalar!(from_f64_bits, value.to_bits()),
                DataType::Float32 => {
                    let narrowed = *value as f32;
                    if f64::from(narrowed).to_bits() != value.to_bits() {
                        return Err(ConstantError::Invalid(
                            "SQL FLOAT syntax is not exact in its declared Float32 carrier",
                        ));
                    }
                    let array = arrow::array::Float32Array::from(vec![narrowed]);
                    work.flush()?;
                    ConstantValue::from_scalar_array(
                        field,
                        value_type.clone(),
                        array.to_data(),
                        policy,
                        phase,
                        work.control(),
                    )
                }
                _ => Err(ConstantError::Invalid(
                    "SQL float syntax requires its declared floating carrier",
                )),
            },
            LiteralValue::Decimal(text) => {
                let (precision, scale) = match value_type.data_type() {
                    DataType::Decimal32(p, s)
                    | DataType::Decimal64(p, s)
                    | DataType::Decimal128(p, s)
                    | DataType::Decimal256(p, s) => (p, s),
                    _ => {
                        return Err(ConstantError::Invalid(
                            "SQL decimal syntax requires its exact declared decimal carrier",
                        ));
                    }
                };
                let coefficient =
                    decimal_coefficient_observed(text, *precision, *scale, &mut work)?;
                work.flush()?;
                macro_rules! decimal {
                    ($native:ty, $array:ty) => {{
                        let coefficient = coefficient
                            .to_i128()
                            .and_then(|value| <$native>::try_from(value).ok())
                            .ok_or(ConstantError::Invalid(
                                "SQL decimal coefficient exceeds its declared width",
                            ))?;
                        let array = <$array>::from(vec![coefficient])
                            .with_precision_and_scale(*precision, *scale)
                            .map_err(|error| ConstantError::Arrow(error.to_string()))?;
                        work.flush()?;
                        ConstantValue::from_scalar_array(
                            field,
                            value_type.clone(),
                            array.to_data(),
                            policy,
                            phase,
                            work.control(),
                        )
                    }};
                }
                match value_type.data_type() {
                    DataType::Decimal32(_, _) => decimal!(i32, arrow::array::Decimal32Array),
                    DataType::Decimal64(_, _) => decimal!(i64, arrow::array::Decimal64Array),
                    DataType::Decimal128(_, _) => decimal!(i128, arrow::array::Decimal128Array),
                    DataType::Decimal256(_, _) => scalar!(from_decimal256, coefficient),
                    _ => unreachable!("declared decimal carrier checked above"),
                }
            }
        }?;
        work.flush()?;
        Ok(checked)
    })();
    if matches!(
        result,
        Err(ConstantError::Control(_) | ConstantError::Limit(_))
    ) {
        return result;
    }
    work.finish()?;
    result
}

/// Read exact plain SQL spelling into the declared unscaled coefficient.
/// Trailing zeros removed by a negative scale never enter a wider temporary
/// integer; long leading-zero spellings stay bounded cooperative byte walks.
pub(crate) fn decimal_coefficient_observed(
    text: &str,
    precision: u8,
    scale: i8,
    work: &mut CompileCheckpoints<'_>,
) -> Result<i256, ConstantError> {
    let bytes = text.as_bytes();
    let negative = bytes.first() == Some(&b'-');
    let start = usize::from(matches!(bytes.first(), Some(b'-' | b'+')));
    let mut dot = None;
    let mut digits = 0usize;
    for (index, byte) in bytes.iter().enumerate().skip(start) {
        work.step()?;
        match byte {
            b'0'..=b'9' => digits += 1,
            b'.' if dot.is_none() => dot = Some(index),
            _ => {
                return Err(ConstantError::Invalid(
                    "SQL decimal syntax must use exact plain decimal notation",
                ));
            }
        }
    }
    if digits == 0 {
        return Err(ConstantError::Invalid(
            "SQL decimal syntax requires a digit",
        ));
    }
    let integer = &bytes[start..dot.unwrap_or(bytes.len())];
    let fraction = dot.map_or(&[][..], |index| &bytes[index + 1..]);
    let integer_kept = if scale >= 0 {
        integer.len()
    } else {
        integer
            .len()
            .saturating_sub(usize::from(scale.unsigned_abs()))
    };
    let fraction_kept = if scale >= 0 {
        fraction.len().min(scale as usize)
    } else {
        0
    };
    for byte in integer[integer_kept..]
        .iter()
        .chain(&fraction[fraction_kept..])
    {
        work.step()?;
        if *byte != b'0' {
            return Err(ConstantError::Invalid(
                "SQL decimal syntax loses digits in its declared scale",
            ));
        }
    }
    let mut coefficient = i256::ZERO;
    let mut significant = 0usize;
    let mut push = |digit: u8, work: &mut CompileCheckpoints<'_>| -> Result<(), ConstantError> {
        work.step()?;
        if significant != 0 || digit != 0 {
            significant += 1;
        }
        if significant > usize::from(precision) {
            return Err(ConstantError::Invalid(
                "SQL decimal coefficient exceeds its declared precision",
            ));
        }
        let multiplied = coefficient.checked_mul(i256::from_i128(10));
        coefficient = if negative {
            multiplied.and_then(|value| value.checked_sub(i256::from_i128(i128::from(digit))))
        } else {
            multiplied.and_then(|value| value.checked_add(i256::from_i128(i128::from(digit))))
        }
        .ok_or(ConstantError::Invalid(
            "SQL decimal coefficient exceeds Decimal256",
        ))?;
        Ok(())
    };
    for byte in integer[..integer_kept]
        .iter()
        .chain(&fraction[..fraction_kept])
    {
        push(*byte - b'0', work)?;
    }
    if scale >= 0 {
        for _ in fraction_kept..scale as usize {
            push(0, work)?;
        }
    }
    Ok(coefficient)
}

#[cfg(any(test, novarocks_dev_controls))]
pub(crate) fn test_constant_policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 1 << 20,
        max_array_nodes: 1 << 20,
        max_logical_elements: 1 << 24,
        max_retained_buffer_bytes: 1 << 30,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 64,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 1 << 30,
        max_library_validation_bytes: 1 << 32,
    }
}

#[cfg(test)]
#[path = "constant_tests.rs"]
mod constant_tests;

fn is_physical_binary(value: &ConstantValue) -> bool {
    let ty = value.value_type();
    ty.logical_type() == novarocks_type_contract::ValueLogicalType::Physical
        && matches!(
            ty.data_type(),
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView
        )
}

// SQL display follows the existing binary literal spelling. This reads only
// the checked ordinal and does not alter the shared diagnostic formatter.
fn write_binary_display(
    selected: Option<&[u8]>,
    work: &mut CompileCheckpoints<'_>,
    output: &mut dyn std::fmt::Write,
) -> Result<std::fmt::Result, crate::compiler::SqlCompileError> {
    let Some(bytes) = selected else {
        work.flush()?;
        let result = output.write_str("NULL");
        for _ in 0..4 {
            work.step()?;
        }
        return Ok(result);
    };
    work.flush()?;
    if output.write_str("X'").is_err() {
        return Ok(Err(std::fmt::Error));
    }
    work.step()?;
    work.step()?;
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    for byte in bytes {
        let pair = [HEX[usize::from(byte >> 4)], HEX[usize::from(byte & 15)]];
        if output
            .write_str(std::str::from_utf8(&pair).expect("hex pair is ASCII"))
            .is_err()
        {
            return Ok(Err(std::fmt::Error));
        }
        work.step()?;
        work.step()?;
    }
    work.flush()?;
    let result = output.write_str("'");
    work.step()?;
    Ok(result)
}

/// SQL display borrows the checked selected value; it is not a semantic key
/// or a representation for re-entering expression binding.
pub(crate) fn format_constant_observed(
    value: &ConstantValue,
    control: &dyn PureCompileControl,
) -> Result<String, crate::compiler::SqlCompileError> {
    use crate::compiler::SqlCompileError;
    if is_physical_binary(value) {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
        let selected = value.try_binary()?;
        let capacity = match selected {
            Some(bytes) => bytes
                .len()
                .checked_mul(2)
                .and_then(|len| len.checked_add(3)),
            None => Some(4),
        }
        .ok_or(SqlCompileError::ResourceExhausted)?;
        let mut text = String::new();
        text.try_reserve_exact(capacity)
            .map_err(|_| SqlCompileError::ResourceExhausted)?;
        write_binary_display(selected, &mut work, &mut text)?
            .map_err(|_| SqlCompileError::ResourceExhausted)?;
        work.finish()?;
        return Ok(text);
    }
    let text = value
        .format_diagnostic_observed(CompilePhase::LowerProgram, control)
        .map_err(SqlCompileError::from)?;
    let ty = value.value_type();
    if ty.logical_type() != novarocks_type_contract::ValueLogicalType::Physical
        || !matches!(
            ty.data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        )
        || value
            .try_utf8_borrowed_observed(CompilePhase::LowerProgram, control)?
            .is_none()
    {
        return Ok(text);
    }
    // SQL display retains string delimiters. This output never becomes a
    // binding key or a new constant, and each copied byte remains observed.
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let capacity = text
        .len()
        .checked_add(2)
        .ok_or(SqlCompileError::ResourceExhausted)?;
    let mut quoted = String::new();
    quoted
        .try_reserve_exact(capacity)
        .map_err(|_| SqlCompileError::ResourceExhausted)?;
    quoted.push('\'');
    work.step()?;
    let mut start = 0;
    while start < text.len() {
        let mut end = start.saturating_add(256).min(text.len());
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        quoted.push_str(&text[start..end]);
        for _ in start..end {
            work.step()?;
        }
        start = end;
    }
    quoted.push('\'');
    work.step()?;
    work.finish()?;
    Ok(quoted)
}

/// Stream selected diagnostic output into the caller's bounded writer.
/// An inner fmt refusal leaves the caller's typed budget journal authoritative.
pub(crate) fn write_constant_observed(
    value: &ConstantValue,
    control: &dyn PureCompileControl,
    output: &mut dyn std::fmt::Write,
) -> Result<std::fmt::Result, crate::compiler::SqlCompileError> {
    if is_physical_binary(value) {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
        let result = write_binary_display(value.try_binary()?, &mut work, output)?;
        work.finish()?;
        return Ok(result);
    }
    let ty = value.value_type();
    let quoted = ty.logical_type() == novarocks_type_contract::ValueLogicalType::Physical
        && matches!(
            ty.data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        )
        && value
            .try_utf8_borrowed_observed(CompilePhase::LowerProgram, control)?
            .is_some();
    if quoted && output.write_str("'").is_err() {
        return Ok(Err(std::fmt::Error));
    }
    let result = value.write_diagnostic_observed(CompilePhase::LowerProgram, control, output)?;
    if result.is_err() {
        return Ok(result);
    }
    if quoted {
        return Ok(output.write_str("'"));
    }
    Ok(Ok(()))
}
