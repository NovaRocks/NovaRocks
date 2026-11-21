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

use novarocks_functions::temporal_value::{
    ParsedTemporal, interval_leading_magnitude, parse_interval,
};
use novarocks_parser::ast;
use novarocks_type_contract::{
    FunctionValueType, IntervalField, IntervalQualifier, TemporalPrecision, TemporalType,
    ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime,
};

use super::TypingError;

/// Select the conversion with its final source, mode and target. The exact
/// installed owner independently validates this pair before preparation.
pub fn select_temporal_cast(
    catalog: &dyn super::ScalarBindingCatalog,
    argument: &novarocks_functions::FunctionArgument,
    target: &FunctionValueType,
    safe: bool,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Option<novarocks_functions::ResolvedFunctionBinding>, TypingError> {
    use novarocks_functions::builtin::value_conversion as owner;
    let novarocks_functions::FunctionArgument::Value { value_type, .. } = argument else {
        return Err(TypingError::InvalidShape("temporal CAST requires a value"));
    };
    if value_type.value_type().temporal().is_none() && target.value_type().temporal().is_none() {
        return Ok(None);
    }
    // Untyped NULL is admitted by the original strict typed NULL author.
    let overload = if value_type.data_type() == &arrow::datatypes::DataType::Null {
        return Ok(Some(
            catalog.select_value_conversion(argument, target, control)?,
        ));
    } else {
        owner::temporal_cast_overload(value_type, target, safe).ok_or_else(|| {
            TypingError::TypeMismatch(format!(
                "cannot cast {:?} to {:?}",
                value_type.value_type().kind(),
                target.value_type().kind()
            ))
        })?
    };
    let function =
        novarocks_type_contract::FunctionId::try_new(owner::VALUE_CONVERSION_FUNCTION_ID)
            .map_err(|_| TypingError::InvalidShape("invalid temporal CAST function identity"))?;
    let overload = novarocks_type_contract::FunctionOverloadId::try_new(overload)
        .map_err(|_| TypingError::InvalidShape("invalid temporal CAST overload identity"))?;
    Ok(Some(catalog.select_exact_scalar(
        &function,
        &overload,
        novarocks_functions::FunctionBindingRequest {
            arguments: std::slice::from_ref(argument),
            logical_argument_count: 1,
            expected_result_type: Some(target),
        },
        control,
    )?))
}

pub use novarocks_functions::builtin::temporal_context::TemporalSyntax;

pub fn clock_syntax(
    expression: &ast::Expr,
) -> Result<Option<(TemporalSyntax, FunctionValueType)>, TypingError> {
    use TemporalSyntax as S;
    let (syntax, precision) = match expression {
        ast::Expr::CurrentDate(_) => {
            return Ok(Some((
                S::CurrentDate,
                FunctionValueType::new(arrow::datatypes::DataType::Date32, false),
            )));
        }
        ast::Expr::CurrentTime(node) => (S::CurrentTime, node.precision),
        ast::Expr::CurrentTimestamp(node) => (S::CurrentTimestamp, node.precision),
        ast::Expr::LocalTime(node) => (S::LocalTime, node.precision),
        ast::Expr::LocalTimestamp(node) => (S::LocalTimestamp, node.precision),
        _ => return Ok(None),
    };
    let precision = match precision {
        None => TemporalPrecision::DEFAULT,
        Some(digits) => u8::try_from(digits)
            .ok()
            .and_then(|digits| TemporalPrecision::new(digits).ok())
            .ok_or_else(|| {
                TypingError::InvalidLiteral(format!("invalid temporal clock precision {digits}"))
            })?,
    };
    let ty = match syntax {
        S::CurrentTime => TemporalType::TimeWithTimeZone(precision),
        S::CurrentTimestamp => TemporalType::TimestampWithTimeZone(precision),
        S::LocalTime => TemporalType::Time(precision),
        S::LocalTimestamp => TemporalType::Timestamp(precision),
        _ => unreachable!("selected clock syntax"),
    };
    Ok(Some((syntax, temporal_function_value_type(ty, false)?)))
}

pub fn at_zone_syntax(
    source: &FunctionValueType,
    zone: Option<&FunctionValueType>,
) -> Result<(TemporalSyntax, FunctionValueType, FunctionValueType), TypingError> {
    use TemporalSyntax as S;
    use TemporalType as T;
    let ty = match source.value_type().temporal() {
        Some(T::Time(p) | T::TimeWithTimeZone(p)) => T::TimeWithTimeZone(p),
        Some(T::Timestamp(p) | T::TimestampWithTimeZone(p)) => T::TimestampWithTimeZone(p),
        _ => {
            return Err(TypingError::TypeMismatch(
                "AT TIME ZONE requires a time or timestamp value".into(),
            ));
        }
    };
    let time = matches!(ty, T::TimeWithTimeZone(_));
    let syntax = match zone {
        None if time => S::AtTimeLocal,
        None => S::AtTimestampLocal,
        Some(zone)
            if zone.data_type() == &arrow::datatypes::DataType::Utf8
                && zone.logical_type() == novarocks_type_contract::ValueLogicalType::Physical =>
        {
            if time {
                S::AtTimeName
            } else {
                S::AtTimestampName
            }
        }
        Some(zone) if matches!(zone.value_type().temporal(), Some(T::Interval(q)) if !q.is_year_month()) => {
            if time {
                S::AtTimeInterval
            } else {
                S::AtTimestampInterval
            }
        }
        _ => {
            return Err(TypingError::TypeMismatch(
                "AT TIME ZONE requires a VARCHAR or day-time interval offset".into(),
            ));
        }
    };
    let zoned_source = temporal_function_value_type(ty, source.nullable())?;
    let result = zoned_source
        .with_nullable(source.nullable() || zone.is_some_and(FunctionValueType::nullable))?;
    Ok((syntax, zoned_source, result))
}

pub fn bind_temporal_syntax(
    catalog: &dyn super::ScalarBindingCatalog,
    syntax: TemporalSyntax,
    arguments: &[novarocks_functions::FunctionArgument],
    target: &FunctionValueType,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<novarocks_functions::ResolvedFunctionBinding, TypingError> {
    let function = novarocks_type_contract::FunctionId::try_new(
        novarocks_functions::builtin::temporal_context::FUNCTION_ID,
    )
    .map_err(|_| TypingError::InvalidShape("invalid temporal syntax identity"))?;
    let overload = novarocks_type_contract::FunctionOverloadId::try_new(syntax.identity())
        .map_err(|_| TypingError::InvalidShape("invalid temporal syntax overload"))?;
    Ok(catalog.select_exact_scalar(
        &function,
        &overload,
        novarocks_functions::FunctionBindingRequest {
            arguments,
            logical_argument_count: arguments.len(),
            expected_result_type: Some(target),
        },
        control,
    )?)
}

/// Author a complete temporal type from the original parser type shape.
pub fn declared_temporal_type(name: &ast::TypeName) -> Result<Option<TemporalType>, TypingError> {
    Ok(Some(match &name.kind {
        ast::TypeKind::DateTime(declaration) => {
            let precision =
                temporal_precision(declaration.precision.as_deref(), TemporalPrecision::DEFAULT)?;
            match (declaration.kind, declaration.with_time_zone) {
                (ast::DateTimeType::Time, false) => TemporalType::Time(precision),
                (ast::DateTimeType::Time, true) => TemporalType::TimeWithTimeZone(precision),
                (ast::DateTimeType::Timestamp, false) => TemporalType::Timestamp(precision),
                (ast::DateTimeType::Timestamp, true) => {
                    TemporalType::TimestampWithTimeZone(precision)
                }
            }
        }
        ast::TypeKind::Interval(declaration) => {
            TemporalType::Interval(declared_interval(declaration)?)
        }
        _ => return Ok(None),
    }))
}

fn declared_interval(
    declaration: &ast::IntervalDataType,
) -> Result<IntervalQualifier, TypingError> {
    let (start, end, leading) = match &declaration.qualifier {
        ast::IntervalQualifier::Simple(declaration) => (
            &declaration.field,
            &declaration.field,
            declaration.precision.as_deref(),
        ),
        ast::IntervalQualifier::Composite(declaration) => (
            &declaration.from,
            &declaration.to,
            declaration.precision.as_deref(),
        ),
    };
    let qualifier = interval_qualifier(start, end, leading, None)?;
    qualifier
        .validate_declared_precision()
        .map_err(|error| TypingError::InvalidLiteral(error.to_string()))?;
    Ok(qualifier)
}

/// Author the exact operand coercions and final interval declaration for a
/// qualified difference. The ordinary SUBTRACT and CAST owners do the work.
pub fn qualified_datetime_difference(
    left: &FunctionValueType,
    right: &FunctionValueType,
    target: &ast::IntervalDataType,
) -> Result<[FunctionValueType; 3], TypingError> {
    use arrow::datatypes::DataType;
    let qualify = |operand: &FunctionValueType| -> Result<Option<FunctionValueType>, TypingError> {
        match operand.value_type().temporal() {
            Some(TemporalType::Interval(_)) => (),
            Some(_) => return Ok(Some(operand.clone())),
            None if operand.data_type() == &DataType::Null => return Ok(None),
            None if operand.data_type() == &DataType::Date32 => return Ok(Some(operand.clone())),
            None => (),
        }
        Err(TypingError::TypeMismatch(
            "Qualified datetime difference requires datetime operands".into(),
        ))
    };
    let operands = match (qualify(left)?, qualify(right)?) {
        (Some(left), Some(right)) => [left, right],
        (Some(left), None) => {
            let right = left.with_nullable(true)?;
            [left, right]
        }
        (None, Some(right)) => {
            let left = right.with_nullable(true)?;
            [left, right]
        }
        (None, None) => {
            return Err(TypingError::TypeMismatch(
                "Qualified datetime difference requires a typed datetime operand".into(),
            ));
        }
    };
    let qualifier = declared_interval(target)?;
    if qualifier.is_year_month() {
        return Err(TypingError::Unsupported(
            "Computing a year-month interval difference of datetimes is not yet supported".into(),
        ));
    }
    let target = temporal_function_value_type(
        TemporalType::Interval(qualifier),
        operands.iter().any(FunctionValueType::nullable),
    )?;
    let [left, right] = operands;
    Ok([left, right, target])
}

pub fn temporal_function_value_type(
    temporal: TemporalType,
    nullable: bool,
) -> Result<FunctionValueType, TypingError> {
    let storage = ValueTypeStorageLifetime::pure();
    let semantic = ValueTypeRef::try_new(ValueKind::Temporal(temporal), nullable, storage.clone())
        .map_err(novarocks_type_contract::ValueTypeError::from)?;
    let layout = ValueLayout::try_new(&temporal.carrier(), storage)
        .map_err(novarocks_type_contract::ValueTypeError::from)?;
    Ok(FunctionValueType::try_from_value(semantic, layout)?)
}

pub(super) fn parse_interval_literal(
    literal: &ast::IntervalLiteral,
) -> Result<ParsedTemporal, TypingError> {
    let (start, end, leading) = match &literal.qualifier {
        ast::IntervalQualifier::Simple(declaration) => (
            &declaration.field,
            &declaration.field,
            declaration.precision.as_deref(),
        ),
        ast::IntervalQualifier::Composite(declaration) => (
            &declaration.from,
            &declaration.to,
            declaration.precision.as_deref(),
        ),
    };
    let invalid = |error: &dyn std::fmt::Display| {
        TypingError::InvalidLiteral(format!(
            "invalid INTERVAL value '{}': {error}",
            literal.value
        ))
    };
    let mut qualifier = interval_qualifier(start, end, leading, None)?;
    if matches!(
        start,
        ast::IntervalQualifierField::Second {
            fractional_precision: None
        }
    ) {
        let digits = literal.value.split_once('.').map_or(0, |(_, tail)| {
            tail.bytes().take_while(u8::is_ascii_digit).count().min(12) as u8
        });
        qualifier = IntervalQualifier::new(
            qualifier.start(),
            qualifier.end(),
            qualifier.leading_precision(),
            TemporalPrecision::new(digits).map_err(|error| invalid(&error))?,
        )
        .map_err(|error| invalid(&error))?;
    }
    let value = parse_interval(
        &literal.value,
        qualifier,
        literal.sign == ast::IntervalSign::Negative,
    )
    .map_err(|error| invalid(&error))?;
    let magnitude = interval_leading_magnitude(value, qualifier.start()).ok_or(
        TypingError::InvalidShape("interval parser returned an incompatible value"),
    )?;
    let value_digits = magnitude
        .checked_ilog10()
        .map_or(1, |digits| digits as u8 + 1);
    if leading.is_none() {
        let written_digits = literal
            .value
            .trim_start_matches(['+', '-'])
            .bytes()
            .take_while(u8::is_ascii_digit)
            .count();
        let max = qualifier.start().max_leading_precision();
        let written_digits = written_digits.clamp(1, usize::from(max)) as u8;
        let inferred = if qualifier.is_year_month() {
            written_digits
        } else {
            written_digits.max(value_digits)
        };
        qualifier = IntervalQualifier::new(
            qualifier.start(),
            qualifier.end(),
            inferred,
            qualifier.fractional_precision(),
        )
        .map_err(|error| invalid(&error))?;
    }
    if value_digits > qualifier.leading_precision() {
        return Err(TypingError::InvalidLiteral(format!(
            "INTERVAL value '{}' exceeds leading precision {}",
            literal.value,
            qualifier.leading_precision(),
        )));
    }
    Ok(ParsedTemporal {
        ty: TemporalType::Interval(qualifier),
        value,
    })
}

pub(crate) fn interval_qualifier(
    start: &ast::IntervalQualifierField,
    end: &ast::IntervalQualifierField,
    leading: Option<&ast::DataTypeParameter>,
    inferred_leading: Option<u8>,
) -> Result<IntervalQualifier, TypingError> {
    let start = interval_field(start);
    let end_field = interval_field(end);
    let fractional = match end {
        ast::IntervalQualifierField::Second {
            fractional_precision,
        } => temporal_precision(
            fractional_precision.as_deref(),
            end_field.default_fractional_precision(),
        )?,
        _ => TemporalPrecision::WHOLE_SECONDS,
    };
    let leading = if let Some(leading) = leading {
        unsigned(leading)?
    } else {
        inferred_leading.unwrap_or(2)
    };
    IntervalQualifier::new(start, end_field, leading, fractional)
        .map_err(|error| TypingError::InvalidLiteral(error.to_string()))
}

fn interval_field(field: &ast::IntervalQualifierField) -> IntervalField {
    match field {
        ast::IntervalQualifierField::Year => IntervalField::Year,
        ast::IntervalQualifierField::Month => IntervalField::Month,
        ast::IntervalQualifierField::Day => IntervalField::Day,
        ast::IntervalQualifierField::Hour => IntervalField::Hour,
        ast::IntervalQualifierField::Minute => IntervalField::Minute,
        ast::IntervalQualifierField::Second { .. } => IntervalField::Second,
    }
}

fn temporal_precision(
    raw: Option<&ast::DataTypeParameter>,
    default: TemporalPrecision,
) -> Result<TemporalPrecision, TypingError> {
    let Some(raw) = raw else { return Ok(default) };
    TemporalPrecision::new(unsigned(raw)?)
        .map_err(|error| TypingError::InvalidLiteral(error.to_string()))
}

fn unsigned(value: &ast::DataTypeParameter) -> Result<u8, TypingError> {
    match value {
        ast::DataTypeParameter::Numeric(value) => u8::try_from(value.parsed_value).map_err(|_| {
            TypingError::InvalidLiteral("temporal precision is outside the supported range".into())
        }),
        _ => Err(TypingError::InvalidLiteral(
            "temporal precision must be an integer".into(),
        )),
    }
}
