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

//! Exact admission of syntax values through the checked constant owner.

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{DataType, i256};
use novarocks_constant_contract::{
    ConstantError, ConstantPolicy, ConstantValue, preflight_scalar_construction,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, FunctionValueType, PureCompileControl,
};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
pub enum LiteralValue {
    Null,
    Bool(bool),
    Int(i64),
    LargeInt(i128),
    Float(f64),
    Decimal(String),
    String(String),
    Binary(Vec<u8>),
}

pub fn admit_syntax_constant(
    value: &LiteralValue,
    value_type: &FunctionValueType,
    policy: ConstantPolicy,
    control: &dyn PureCompileControl,
) -> Result<ConstantValue, ConstantError> {
    let phase = CompilePhase::FunctionSpecialization;
    let mut work = CompileCheckpoints::try_new(control, phase)?;
    let result = (|| {
        validate_syntax_type(value_type, &mut work)?;
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
                        let value = syntax_integer_value::<$native>(*value)?;
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
                    let narrowed = syntax_float32_value(*value)?;
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
                            .map_err(ConstantError::arrow_validation)?;
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
    work.finish_result(result, ConstantError::from)
}

// First scalar and VALUES run creation share the same original syntax
// checks. They keep their own actual construction recipe and backing owner.
pub fn validate_syntax_type(
    value_type: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ConstantError> {
    novarocks_functions::validate_function_value_type_observed(value_type, work).map_err(
        |error| match error {
            novarocks_functions::KernelFailure::CompileControl(cause) => {
                ConstantError::Control(cause)
            }
            novarocks_functions::KernelFailure::Cancelled => {
                ConstantError::Control(novarocks_type_contract::CompileControlError::Cancelled)
            }
            novarocks_functions::KernelFailure::DeadlineExceeded => ConstantError::Control(
                novarocks_type_contract::CompileControlError::DeadlineExceeded,
            ),
            novarocks_functions::KernelFailure::ResourceExhausted => ConstantError::Control(
                novarocks_type_contract::CompileControlError::ResourceExhausted,
            ),
            error => ConstantError::from_domain(error),
        },
    )?;
    Ok(())
}
pub fn syntax_integer_value<T: TryFrom<i64>>(value: i64) -> Result<T, ConstantError> {
    T::try_from(value)
        .map_err(|_| ConstantError::Invalid("SQL integer syntax exceeds its declared carrier"))
}
pub fn syntax_float32_value(value: f64) -> Result<f32, ConstantError> {
    let narrowed = value as f32;
    if f64::from(narrowed).to_bits() != value.to_bits() {
        return Err(ConstantError::Invalid(
            "SQL FLOAT syntax is not exact in its declared Float32 carrier",
        ));
    }
    Ok(narrowed)
}

/// Read exact plain SQL spelling into the declared unscaled coefficient.
/// Trailing zeros removed by a negative scale never enter a wider temporary
/// integer; long leading-zero spellings stay bounded cooperative byte walks.
pub fn decimal_coefficient_observed(
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
