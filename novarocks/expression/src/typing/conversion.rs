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

use arrow::datatypes::{DataType, TimeUnit};

use novarocks_functions::{
    FunctionResultType, builtin::value_conversion::conversion_intermediate_type,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, DecimalOverflowPolicy, FunctionValueType, ValueLogicalType,
    preserves_value_domains_observed,
};

use super::{
    CarrierConversion, ChildForm, ChildTyping, Conversion, LiteralValue, TypedLiteral,
    TypingContext, TypingError, ValueConversionOrigin, ValueDomainConversion,
};

pub(super) fn typed_null(
    context: &TypingContext<'_>,
    target: FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Conversion, TypingError> {
    TypedLiteral::checked(
        LiteralValue::Null,
        target.with_nullable(true)?,
        context.constant_policy,
        work,
    )
    .map(Conversion::RetypeLiteral)
}

pub(super) fn coerce_argument(
    context: &TypingContext<'_>,
    child: &ChildTyping,
    target: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Conversion, TypingError> {
    let source = &child.value_type;
    if novarocks_type_contract::trino_can_coerce(source.value_type(), target.value_type(), work)? {
        if same_type(source, target, work)? {
            return Ok(Conversion::Keep);
        }
        if is_null(child, work)? {
            return typed_null(context, target.clone(), work);
        }
        if source.value_type().temporal().is_some() || target.value_type().temporal().is_some() {
            return value_domain(context, child, target.clone(), work);
        }
        return super::type_cast(
            context,
            source,
            target.clone(),
            novarocks_type_contract::TrinoCastMode::Implicit,
            work,
        )
        .map(Conversion::Cast);
    }
    let same_domain = source.same_value_domain_observed::<TypingError>(target, || {
        work.step()?;
        Ok(())
    })?;
    let same_carrier = source.data_type() == target.data_type();
    let cast_nullable = !same_carrier
        && ((source.logical_type() == ValueLogicalType::Physical
            && target.logical_type() == ValueLogicalType::Physical
            && novarocks_functions::carrier_cast_can_produce_null_with_policy(
                source.data_type(),
                target.data_type(),
                context.decimal_overflow_policy,
                false,
            ))
            || timestamp_unit_cast_can_return_null(source.data_type(), target.data_type()));
    let target = target.with_nullable(source.nullable() || cast_nullable)?;

    if !same_domain {
        if is_null(child, work)? {
            return typed_null(context, target, work);
        }
        if !preserves_domains(child, &target, work)? {
            let target = target.with_nullable(
                target.nullable()
                    || (source.logical_type() == ValueLogicalType::LargeInt
                        && target.logical_type() == ValueLogicalType::Physical
                        && signed_integer(target.data_type())),
            )?;
            return value_domain(context, child, target, work);
        }
    }

    if !same_carrier
        && let ChildForm::IntegerLiteral(value) | ChildForm::NegatedIntegerLiteral(value) =
            child.form
        && target.logical_type() == ValueLogicalType::Physical
        && signed_integer(target.data_type())
    {
        let target = target.with_nullable(source.nullable())?;
        check_integer(value, target.data_type())?;
        return TypedLiteral::checked(
            LiteralValue::Int(value),
            target,
            context.constant_policy,
            work,
        )
        .map(Conversion::RetypeLiteral);
    }

    carrier_or_keep(context.decimal_overflow_policy, child, target, work)
}

/// Materialize the collection target selected by an exact ARRAY overload.
pub(super) fn array_common(
    context: &TypingContext<'_>,
    child: &ChildTyping,
    target: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Conversion, TypingError> {
    use novarocks_functions::builtin::value_conversion::{
        ARRAY_COMMON_COERCION, VALUE_CONVERSION_FUNCTION_ID,
    };
    use novarocks_type_contract::{FunctionId, FunctionOverloadId};

    if same_type(&child.value_type, target, work)? {
        return Ok(Conversion::Keep);
    }
    let function = FunctionId::try_new(VALUE_CONVERSION_FUNCTION_ID)
        .expect("valid builtin value conversion identity");
    let overload = FunctionOverloadId::try_new(ARRAY_COMMON_COERCION)
        .expect("valid ARRAY common conversion overload identity");
    let arguments = [child.argument()];
    work.flush()?;
    let binding = context.catalog.select_exact_scalar(
        &function,
        &overload,
        super::request(&arguments, Some(target)),
        work.control(),
    )?;
    if binding.function_id != function
        || binding.selected.overload != overload
        || !matches!(&binding.selected.result_type, FunctionResultType::Scalar(result)
            if same_type(result, target, work)?)
    {
        return Err(TypingError::InvalidShape(
            "ARRAY common conversion differs from its exact selected target",
        ));
    }
    Ok(Conversion::ValueDomain(Box::new(ValueDomainConversion {
        binding,
        expected_result: target.clone(),
        target: target.clone(),
        tail: None,
        origin: ValueConversionOrigin::ArrayCommonCoercion,
    })))
}

/// Convert a Decimal128 operand in an exact floating operator domain.
pub fn operator_decimal_float(
    context: &TypingContext<'_>,
    child: &ChildTyping,
    target: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Conversion, TypingError> {
    use novarocks_functions::builtin::value_conversion::{
        OPERATOR_DECIMAL_FLOAT_COERCION, VALUE_CONVERSION_FUNCTION_ID,
    };
    use novarocks_type_contract::{FunctionId, FunctionOverloadId};

    let function = FunctionId::try_new(VALUE_CONVERSION_FUNCTION_ID)
        .expect("valid builtin value conversion identity");
    let overload = FunctionOverloadId::try_new(OPERATOR_DECIMAL_FLOAT_COERCION)
        .expect("valid operator coercion overload identity");
    let arguments = [child.argument()];
    work.flush()?;
    let binding = context.catalog.select_exact_scalar(
        &function,
        &overload,
        super::request(&arguments, Some(target)),
        work.control(),
    )?;
    if binding.function_id != function
        || binding.selected.overload != overload
        || !matches!(&binding.selected.result_type, FunctionResultType::Scalar(result)
            if result.exactly_equals_checked_observed(target, || work.step())?)
    {
        return Err(TypingError::InvalidShape(
            "operator coercion differs from its exact selected target",
        ));
    }
    Ok(Conversion::ValueDomain(Box::new(ValueDomainConversion {
        binding,
        expected_result: target.clone(),
        target: target.clone(),
        tail: None,
        origin: ValueConversionOrigin::OperatorDecimalFloat,
    })))
}

/// Publish the complete CASE branch target, including identity-domain
/// conversions that widen the branch declaration's nullability.
pub(super) fn assign(
    context: &TypingContext<'_>,
    child: &ChildTyping,
    target: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Conversion, TypingError> {
    let source = &child.value_type;
    if same_type(source, target, work)? {
        return Ok(Conversion::Keep);
    }
    if is_null(child, work)? {
        return typed_null(context, target.clone(), work);
    }
    if preserves_domains(child, target, work)?
        || novarocks_type_contract::is_default_json_structure_cast(source, target)
        || novarocks_type_contract::is_null_list_to_json_cast(source, target)
    {
        // The old same-domain conversion was emitted by CASE lowering with
        // the existing OutputNull policy. Differing domains use the session policy.
        let decimal_overflow_policy =
            if source.same_value_domain_observed::<TypingError>(target, || {
                work.step()?;
                Ok(())
            })? {
                DecimalOverflowPolicy::OutputNull
            } else {
                context.decimal_overflow_policy
            };
        return Ok(Conversion::Cast(CarrierConversion {
            mode: None,
            target: target.clone(),
            decimal_overflow_policy,
        }));
    }
    value_domain(context, child, target.clone(), work)
}

fn carrier_or_keep(
    decimal_overflow_policy: DecimalOverflowPolicy,
    child: &ChildTyping,
    target: FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Conversion, TypingError> {
    if same_type(&child.value_type, &target, work)? {
        Ok(Conversion::Keep)
    } else {
        Ok(Conversion::Cast(CarrierConversion {
            mode: None,
            target,
            decimal_overflow_policy,
        }))
    }
}

fn same_type(
    left: &FunctionValueType,
    right: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, TypingError> {
    Ok(left.exactly_equals_checked_observed(right, || work.step())?)
}

fn is_null(child: &ChildTyping, work: &mut CompileCheckpoints<'_>) -> Result<bool, TypingError> {
    if child.form == ChildForm::NullLiteral {
        return Ok(true);
    }
    let Some(constant) = &child.constant else {
        return Ok(false);
    };
    work.flush()?;
    Ok(constant.is_null_observed(CompilePhase::FunctionSpecialization, work.control())?)
}

fn preserves_domains(
    child: &ChildTyping,
    target: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, TypingError> {
    work.flush()?;
    Ok(preserves_value_domains_observed(
        child.value_type.value_type(),
        target.value_type(),
        |units| {
            for _ in 0..units {
                work.step()?;
            }
            Ok::<_, novarocks_type_contract::CompileControlError>(())
        },
    )?)
}

fn value_domain(
    context: &TypingContext<'_>,
    child: &ChildTyping,
    target: FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Conversion, TypingError> {
    work.flush()?;
    let expected_result = conversion_intermediate_type(&child.value_type, &target)?.ok_or(
        TypingError::InvalidShape("logical-domain conversion has no exact owner"),
    )?;
    let binding = context.catalog.select_value_conversion(
        &child.argument(),
        &expected_result,
        work.control(),
    )?;
    let FunctionResultType::Scalar(result) = &binding.selected.result_type else {
        return Err(TypingError::InvalidShape(
            "value conversion must return a scalar",
        ));
    };
    let tail = if same_type(result, &target, work)? {
        None
    } else {
        Some(CarrierConversion {
            mode: None,
            target: target.clone(),
            decimal_overflow_policy: context.decimal_overflow_policy,
        })
    };
    Ok(Conversion::ValueDomain(Box::new(ValueDomainConversion {
        binding,
        expected_result,
        target,
        tail,
        origin: ValueConversionOrigin::ValueDomain,
    })))
}

fn signed_integer(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

fn check_integer(value: i64, ty: &DataType) -> Result<(), TypingError> {
    let target = match ty {
        DataType::Int8 if i8::try_from(value).is_err() => "tinyint",
        DataType::Int16 if i16::try_from(value).is_err() => "smallint",
        DataType::Int32 if i32::try_from(value).is_err() => "int",
        _ => return Ok(()),
    };
    Err(TypingError::TypeMismatch(format!(
        "Cast argument {value} to {target} type failed"
    )))
}

// The existing safe Arrow cast can convert these unit-expansion overflows
// to NULL. The microsecond-to-nanosecond owner reports overflow as an error.
fn timestamp_unit_cast_can_return_null(source: &DataType, target: &DataType) -> bool {
    matches!(
        (source, target),
        (
            DataType::Timestamp(TimeUnit::Second, _),
            DataType::Timestamp(
                TimeUnit::Millisecond | TimeUnit::Microsecond | TimeUnit::Nanosecond,
                _
            )
        ) | (
            DataType::Timestamp(TimeUnit::Millisecond, _),
            DataType::Timestamp(TimeUnit::Microsecond | TimeUnit::Nanosecond, _)
        )
    )
}
