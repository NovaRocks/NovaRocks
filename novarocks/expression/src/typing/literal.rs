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

use std::{cell::Cell, sync::Arc};

use arrow::array::{Array, LargeBinaryArray};
use arrow::buffer::{Buffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field};
use novarocks_constant_contract::{ConstantError, ConstantPolicy, ConstantValue};
use novarocks_functions::temporal_value::{
    ParsedTemporal, parse_time_literal, parse_timestamp_literal,
};
use novarocks_functions::{
    KernelFailure,
    json_value::{self, JsonError, JsonWork},
};
use novarocks_parser::ast;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionValueType, ValueLogicalType,
};

use super::{CarrierConversion, LiteralValue, TypingError, admit_syntax_constant};

// Compilation admits permanent constant bytes and parser scratch against the
// original statement's ConstantPolicy, independently of its work checkpoints.
struct LiteralJsonWork<'a, 'c> {
    compile: &'a mut CompileCheckpoints<'c>,
    policy: ConstantPolicy,
    scratch: &'a Cell<usize>,
}
struct LiteralScratch<'a> {
    live: &'a Cell<usize>,
    bytes: usize,
}
impl Drop for LiteralScratch<'_> {
    fn drop(&mut self) {
        self.live.set(self.live.get() - self.bytes);
    }
}
impl<'a> JsonWork for LiteralJsonWork<'a, '_> {
    type Reservation = LiteralScratch<'a>;

    fn step(&mut self) -> Result<(), KernelFailure> {
        self.compile.step().map_err(KernelFailure::CompileControl)
    }

    fn reserve(&mut self, bytes: usize) -> Result<Self::Reservation, KernelFailure> {
        self.compile
            .flush()
            .map_err(KernelFailure::CompileControl)?;
        let live = self
            .scratch
            .get()
            .checked_add(bytes)
            .filter(|live| {
                u64::try_from(*live)
                    .is_ok_and(|live| live <= self.policy.max_library_validation_bytes)
            })
            .ok_or(KernelFailure::ResourceExhausted)?;
        self.scratch.set(live);
        Ok(LiteralScratch {
            live: self.scratch,
            bytes,
        })
    }

    fn reserve_output(&mut self, bytes: usize) -> Result<(), KernelFailure> {
        self.compile
            .flush()
            .map_err(KernelFailure::CompileControl)?;
        let retained = bytes
            .checked_add(size_of::<[i64; 2]>())
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or(KernelFailure::ResourceExhausted)?;
        if retained > self.policy.max_retained_buffer_bytes {
            return Err(KernelFailure::ResourceExhausted);
        }
        Ok(())
    }
}

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

pub fn supports_typed_string(name: &str) -> bool {
    super::declaration::supports_literal_type_name(name)
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
        LiteralSyntax::String(value) => {
            return string_literal(context, value, work).map(LiteralEmission::Direct);
        }
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
            use super::declaration::{TypeDeclarationProblem, resolve_trino_type};
            use novarocks_type_contract::TrinoCastMode;

            let syntax = novarocks_parser::build::simple_type(name);
            let target =
                resolve_trino_type(&syntax, false, work.control()).map_err(|error| match error
                    .problem
                {
                    TypeDeclarationProblem::UnknownType
                    | TypeDeclarationProblem::InvalidParameters
                    | TypeDeclarationProblem::InvalidDecimalParameters => {
                        TypingError::TypeMismatch(error.to_string())
                    }
                    TypeDeclarationProblem::Unsupported => {
                        TypingError::Unsupported(error.to_string())
                    }
                    TypeDeclarationProblem::Control => TypingError::Control(
                        error.control.expect("original type declaration control"),
                    ),
                })?;
            if target.logical_type() == ValueLogicalType::Uuid {
                let bytes = novarocks_functions::parse_uuid(value)
                    .ok_or_else(|| TypingError::InvalidLiteral("Invalid UUID literal".into()))?;
                let field = Arc::new(Field::new("literal", DataType::FixedSizeBinary(16), false));
                work.flush()?;
                let value = ConstantValue::from_uuid(
                    field,
                    target.clone(),
                    bytes,
                    context.constant_policy,
                    CompilePhase::FunctionSpecialization,
                    work.control(),
                )?;
                return Ok(LiteralEmission::Constant {
                    value_type: target,
                    value,
                });
            }
            if target.logical_type() == ValueLogicalType::Json {
                work.flush()?;
                let scratch = Cell::new(0);
                let mut json_work = LiteralJsonWork {
                    compile: work,
                    policy: context.constant_policy,
                    scratch: &scratch,
                };
                let bytes =
                    json_value::parse(value, &mut json_work).map_err(|error| match error {
                        JsonError::InvalidText(_) | JsonError::Depth => {
                            TypingError::InvalidLiteral(error.to_string())
                        }
                        JsonError::Host(KernelFailure::CompileControl(error)) => {
                            TypingError::Control(error)
                        }
                        JsonError::Host(KernelFailure::ResourceExhausted) => {
                            TypingError::Control(CompileControlError::ResourceExhausted)
                        }
                        JsonError::Host(error) => TypingError::JsonLiteralValidation(error),
                        JsonError::Value(cause) => {
                            TypingError::JsonLiteralValidation(KernelFailure::from_domain(cause))
                        }
                    })?;
                let end = i64::try_from(bytes.len())
                    .map_err(|_| ConstantError::Limit("JSON literal exceeds its Arrow offset"))?;
                let array = LargeBinaryArray::new(
                    OffsetBuffer::new(vec![0i64, end].into()),
                    Buffer::from_vec(bytes),
                    None,
                );
                work.flush()?;
                let value = ConstantValue::from_scalar_array(
                    Arc::new(Field::new("literal", DataType::LargeBinary, false)),
                    target.clone(),
                    array.to_data(),
                    context.constant_policy,
                    CompilePhase::FunctionSpecialization,
                    work.control(),
                )?;
                return Ok(LiteralEmission::Constant {
                    value_type: target,
                    value,
                });
            }
            if target.data_type() == &DataType::Date32 {
                let days = novarocks_functions::temporal_value::parse_date_literal_observed(
                    value,
                    || work.step(),
                )?
                .ok_or_else(|| {
                    TypingError::InvalidLiteral(format!("invalid DATE literal '{value}'"))
                })?;
                return TypedLiteral::checked(
                    LiteralValue::Int(i64::from(days)),
                    target,
                    context.constant_policy,
                    work,
                )
                .map(LiteralEmission::Direct);
            }
            let input = string_literal(context, value, work)?;
            let conversion = super::type_cast(
                context,
                &input.value_type,
                target,
                TrinoCastMode::Explicit,
                work,
            )?;
            return Ok(LiteralEmission::Cast {
                input,
                declared_result: conversion.target.clone(),
                conversion,
            });
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

fn string_literal(
    context: &super::TypingContext<'_>,
    text: &str,
    work: &mut CompileCheckpoints<'_>,
) -> Result<TypedLiteral, TypingError> {
    use novarocks_type_contract::{
        CharacterKind, CharacterType, ValueKind, ValueLayout, ValueTypeRef,
        ValueTypeStorageLifetime,
    };

    let mut length = 0usize;
    for _ in text.chars() {
        work.step()?;
        length += 1;
    }
    let kind = u32::try_from(length)
        .ok()
        .and_then(|length| CharacterType::new(CharacterKind::Varchar, length))
        .map_or(ValueKind::String, ValueKind::BoundedCharacter);
    let storage = ValueTypeStorageLifetime::pure();
    let ty = ValueTypeRef::try_new(kind, false, storage.clone())
        .map_err(novarocks_type_contract::ValueTypeError::from)?;
    let layout = ValueLayout::try_from_normalized_arrow(DataType::Utf8, storage)
        .map_err(novarocks_type_contract::ValueTypeError::from)?;
    let ty = FunctionValueType::try_from_value(ty, layout)?;
    TypedLiteral::checked(
        LiteralValue::String(text.to_owned()),
        ty,
        context.constant_policy,
        work,
    )
}

fn infer_decimal_literal_type(s: &str) -> Result<DataType, TypingError> {
    let raw = s.trim().trim_start_matches('+').trim_start_matches('-');
    let (integer, fraction) = raw.split_once('.').unwrap_or((raw, ""));
    let int_digits = integer.trim_start_matches('0').len();
    let scale = fraction.len();
    let precision = (int_digits + scale).max(1);
    if scale > 38 {
        return Err(TypingError::InvalidLiteral(format!(
            "decimal literal scale {scale} exceeds maximum scale 38: {s}"
        )));
    }
    if precision > 38 {
        return Err(TypingError::InvalidLiteral(format!(
            "decimal literal precision {precision} exceeds maximum precision 38: {s}"
        )));
    }
    let precision = precision as u8;
    let scale = scale as i8;
    Ok(DataType::Decimal128(precision, scale))
}
