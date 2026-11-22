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

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, TimeUnit};
use novarocks_constant_contract::{ConstantPolicy, ConstantValue};
use novarocks_functions::temporal_value::{
    ParsedTemporal, parse_time_literal, parse_timestamp_literal,
};
use novarocks_parser::ast;
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, FunctionValueType, ValueLogicalType,
};

use super::{CarrierConversion, LiteralValue, TypingError, admit_syntax_constant};

/// The skeleton supplies decoded text and parser-admitted values.
#[derive(Clone, Copy, Debug)]
pub enum LiteralSyntax<'a> {
    Null,
    Boolean(bool),
    Integer(i64),
    Double(f64),
    Decimal(&'a str),
    String(&'a str),
    Binary(&'a [u8]),
    TypedString { name: &'a str, value: &'a str },
    Interval(&'a ast::IntervalLiteral),
}

#[derive(Clone, Debug)]
pub struct TypedLiteral {
    pub value: LiteralValue,
    pub value_type: FunctionValueType,
    pub constant: ConstantValue,
}

impl TypedLiteral {
    pub(super) fn checked(
        value: LiteralValue,
        value_type: FunctionValueType,
        policy: ConstantPolicy,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, TypingError> {
        work.flush()?;
        let constant = admit_syntax_constant(&value, &value_type, policy, work.control())?;
        Ok(Self {
            value,
            value_type,
            constant,
        })
    }
}

#[derive(Debug)]
pub enum LiteralEmission {
    Direct(TypedLiteral),
    Constant {
        value_type: FunctionValueType,
        value: ConstantValue,
    },
    Cast {
        input: TypedLiteral,
        declared_result: FunctionValueType,
        conversion: CarrierConversion,
    },
}

fn typed_string(name: &str) -> Option<DataType> {
    if name.eq_ignore_ascii_case("date") {
        Some(DataType::Date32)
    } else if name.eq_ignore_ascii_case("time") {
        Some(DataType::Time64(TimeUnit::Microsecond))
    } else if name.eq_ignore_ascii_case("timestamp") || name.eq_ignore_ascii_case("datetime") {
        Some(DataType::Timestamp(TimeUnit::Microsecond, None))
    } else if name.eq_ignore_ascii_case("json") {
        Some(DataType::Utf8)
    } else {
        None
    }
}

pub fn supports_typed_string(name: &str) -> bool {
    typed_string(name).is_some()
}

pub(super) fn type_literal(
    context: &super::TypingContext<'_>,
    literal: LiteralSyntax<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<LiteralEmission, TypingError> {
    work.step()?;
    let (value, data_type, nullable) = match literal {
        LiteralSyntax::Null => (LiteralValue::Null, DataType::Null, true),
        LiteralSyntax::Boolean(value) => (LiteralValue::Bool(value), DataType::Boolean, false),
        LiteralSyntax::Integer(value) => {
            let ty = if i32::try_from(value).is_ok() {
                DataType::Int32
            } else {
                DataType::Int64
            };
            (LiteralValue::Int(value), ty, false)
        }
        LiteralSyntax::Double(value) => (LiteralValue::Float(value), DataType::Float64, false),
        LiteralSyntax::Decimal(value) => {
            for _ in value.bytes() {
                work.step()?;
            }
            (
                LiteralValue::Decimal(value.to_owned()),
                infer_decimal_literal_type(value)?,
                false,
            )
        }
        LiteralSyntax::String(value) => (
            LiteralValue::String(value.to_owned()),
            DataType::Utf8,
            false,
        ),
        LiteralSyntax::Binary(value) => (
            LiteralValue::Binary(value.to_vec()),
            DataType::Binary,
            false,
        ),
        LiteralSyntax::TypedString { name, value } => {
            let is_time = name.eq_ignore_ascii_case("time");
            if is_time || name.eq_ignore_ascii_case("timestamp") {
                for _ in value.bytes() {
                    work.step()?;
                }
                let parsed = if is_time {
                    parse_time_literal(value)
                } else {
                    parse_timestamp_literal(value)
                }
                .map_err(|error| {
                    TypingError::InvalidLiteral(format!(
                        "invalid {name} literal '{value}': {error}"
                    ))
                })?;
                return temporal_constant(context, parsed, work);
            }
            let target = typed_string(name).ok_or_else(|| {
                TypingError::Unsupported(format!("generic literal type {name} is not supported"))
            })?;
            if name.eq_ignore_ascii_case("json") {
                let result = FunctionValueType::try_with_logical_type(
                    DataType::Utf8,
                    false,
                    ValueLogicalType::Json,
                )?;
                return TypedLiteral::checked(
                    LiteralValue::String(value.to_owned()),
                    result,
                    context.constant_policy,
                    work,
                )
                .map(LiteralEmission::Direct);
            }
            if target == DataType::Date32 {
                let days = novarocks_functions::temporal_value::parse_date_literal_observed(
                    value,
                    || work.step(),
                )?
                .ok_or_else(|| {
                    TypingError::InvalidLiteral(format!("invalid DATE literal '{value}'"))
                })?;
                (LiteralValue::Int(i64::from(days)), target, false)
            } else {
                // SQL has a non-null temporal literal declaration. The checked
                // physical text cast can produce NULL and must retain that fact.
                let nullable = novarocks_functions::carrier_cast_can_produce_null_with_policy(
                    &DataType::Utf8,
                    &target,
                    context.decimal_overflow_policy,
                    false,
                );
                return Ok(LiteralEmission::Cast {
                    input: TypedLiteral::checked(
                        LiteralValue::String(value.to_owned()),
                        FunctionValueType::new(DataType::Utf8, false),
                        context.constant_policy,
                        work,
                    )?,
                    declared_result: FunctionValueType::new(target.clone(), false),
                    conversion: CarrierConversion {
                        target: FunctionValueType::new(target, nullable),
                        decimal_overflow_policy: context.decimal_overflow_policy,
                    },
                });
            }
        }
        LiteralSyntax::Interval(interval) => {
            for _ in interval.value.bytes() {
                work.step()?;
            }
            return temporal_constant(
                context,
                super::temporal::parse_interval_literal(interval)?,
                work,
            );
        }
    };
    TypedLiteral::checked(
        value,
        FunctionValueType::new(data_type, nullable),
        context.constant_policy,
        work,
    )
    .map(LiteralEmission::Direct)
}

fn temporal_constant(
    context: &super::TypingContext<'_>,
    parsed: ParsedTemporal,
    work: &mut CompileCheckpoints<'_>,
) -> Result<LiteralEmission, TypingError> {
    work.flush()?;
    let value_type = super::temporal_function_value_type(parsed.ty, false)?;
    let value = ConstantValue::from_temporal(
        Arc::new(Field::new("value", parsed.ty.carrier(), false)),
        value_type.clone(),
        parsed.value,
        context.constant_policy,
        CompilePhase::FunctionSpecialization,
        work.control(),
    )?;
    Ok(LiteralEmission::Constant { value_type, value })
}

fn infer_decimal_literal_type(s: &str) -> Result<DataType, TypingError> {
    let raw = s.trim().trim_start_matches('+').trim_start_matches('-');
    let (integer, fraction) = raw.split_once('.').unwrap_or((raw, ""));
    let int_digits = integer.trim_start_matches('0').len().max(1);
    let scale = fraction.len();
    let precision = int_digits + scale;
    if scale > 76 {
        return Err(TypingError::InvalidLiteral(format!(
            "decimal literal scale {scale} exceeds maximum scale 76: {s}"
        )));
    }
    if precision > 76 {
        return Err(TypingError::InvalidLiteral(format!(
            "decimal literal precision {precision} exceeds maximum precision 76: {s}"
        )));
    }
    let precision = precision as u8;
    let scale = scale as i8;
    if precision > 38 {
        Ok(DataType::Decimal256(precision, scale))
    } else {
        Ok(DataType::Decimal128(precision, scale))
    }
}
