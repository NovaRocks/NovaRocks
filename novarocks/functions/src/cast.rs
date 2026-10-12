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

//! Immutable selected casts for exact Physical numeric and unzoned timestamp domains.
//! The caller retains original policies and full types; this recipe performs no
//! registry lookup, coercion or memory admission. Temporal Arrow casts retain
//! the original allocation body, bounded to one already selected row.

use crate::kernel_control::{internal, invalid};
use crate::kernel_input::{EvaluationCheckpoints, logical_is_null, validate_type_observed};
use crate::{
    EvaluatedArgument, KernelEvaluationControl, KernelFailure, RowDataError,
    ScopedExpressionEffects,
};
use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow_cast::cast::{cast_num_to_bool, num_cast};
use arrow_schema::{DataType, TimeUnit};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, DecimalOverflowPolicy,
    ExpressionEffectContext, ExpressionEffects, FunctionValueType, PureCompileControl,
    ValueLogicalType,
};
use std::{error::Error, fmt};

#[path = "cast_calendar.rs"]
mod calendar;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CastOperation {
    Carrier,
    Time,
    TimeFromDatetime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CastPrepareError {
    Control(CompileControlError),
    Kernel(KernelFailure),
    Unsupported,
    TypeMismatch,
}
impl CastPrepareError {
    pub fn control_error(&self) -> Option<CompileControlError> {
        match self {
            Self::Control(error) => Some(*error),
            Self::Kernel(KernelFailure::Cancelled) => Some(CompileControlError::Cancelled),
            Self::Kernel(KernelFailure::DeadlineExceeded) => {
                Some(CompileControlError::DeadlineExceeded)
            }
            Self::Kernel(KernelFailure::ResourceExhausted) => {
                Some(CompileControlError::ResourceExhausted)
            }
            _ => None,
        }
    }
}
impl From<CompileControlError> for CastPrepareError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl fmt::Display for CastPrepareError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(f),
            Self::Kernel(error) => error.fmt(f),
            Self::Unsupported => {
                f.write_str("cast requires an implemented exact Physical scalar domain")
            }
            Self::TypeMismatch => {
                f.write_str("cast result violates its frozen successful-NULL contract")
            }
        }
    }
}
impl Error for CastPrepareError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Control(error) => Some(error),
            Self::Kernel(error) => Some(error),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum SignedWidth {
    I8,
    I16,
    I32,
    I64,
}
impl SignedWidth {
    fn from_type(ty: &DataType) -> Option<Self> {
        match ty {
            DataType::Int8 => Some(Self::I8),
            DataType::Int16 => Some(Self::I16),
            DataType::Int32 => Some(Self::I32),
            DataType::Int64 => Some(Self::I64),
            _ => None,
        }
    }
    fn validate(self, array: &dyn Array) -> bool {
        match self {
            Self::I8 => array.as_any().is::<Int8Array>(),
            Self::I16 => array.as_any().is::<Int16Array>(),
            Self::I32 => array.as_any().is::<Int32Array>(),
            Self::I64 => array.as_any().is::<Int64Array>(),
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum UnsignedWidth {
    U8,
    U16,
    U32,
    U64,
}
impl UnsignedWidth {
    fn from_type(ty: &DataType) -> Option<Self> {
        match ty {
            DataType::UInt8 => Some(Self::U8),
            DataType::UInt16 => Some(Self::U16),
            DataType::UInt32 => Some(Self::U32),
            DataType::UInt64 => Some(Self::U64),
            _ => None,
        }
    }
    fn validate(self, array: &dyn Array) -> bool {
        match self {
            Self::U8 => array.as_any().is::<UInt8Array>(),
            Self::U16 => array.as_any().is::<UInt16Array>(),
            Self::U32 => array.as_any().is::<UInt32Array>(),
            Self::U64 => array.as_any().is::<UInt64Array>(),
        }
    }
}

/// Whether an exact primitive signed integer cast can return successful NULL.
/// This policy-independent fact covers only signed narrowing; false outside
/// that domain does not declare a cast supported or safe from other failures.
pub fn signed_integer_cast_can_produce_null(source: &DataType, target: &DataType) -> bool {
    match (
        SignedWidth::from_type(source),
        SignedWidth::from_type(target),
    ) {
        (Some(source), Some(target)) => target < source,
        _ => false,
    }
}

/// Successful-NULL obligations of exact primitive carrier casts. This is a
/// static semantic fact, not an installed runtime capability whitelist.
pub fn carrier_cast_can_produce_null(source: &DataType, target: &DataType, allow: bool) -> bool {
    if matches!(
        source,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) && target == &DataType::Time64(TimeUnit::Microsecond)
    {
        return true;
    }
    if target == &DataType::Time64(TimeUnit::Microsecond)
        && crate::time_calendar_cast::can_produce_null(source)
    {
        return true;
    }
    if target == &DataType::Float32
        && matches!(source, DataType::Decimal128(_, scale) if *scale < 0)
    {
        return true;
    }
    if source == &DataType::Float64 && matches!(target, DataType::Decimal128(..)) {
        return true;
    }
    if source == &DataType::Binary && target == &DataType::Utf8 {
        return true;
    }
    // These original arena branches reject invalid/nonfinite literals even with false ALLOW.
    if target == &DataType::Date32 && matches!(source, DataType::Float32 | DataType::Float64) {
        return false;
    }
    if matches!(target, DataType::Date32 | DataType::Timestamp(_, None))
        && !matches!(source, DataType::Date32 | DataType::Timestamp(_, _))
        && Source::from_type(source).is_some()
    {
        return true;
    }
    if source == &DataType::Utf8
        && (SignedWidth::from_type(target).is_some() || target == &DataType::Boolean)
    {
        return true;
    }
    let integer = |ty: &DataType| match ty {
        DataType::Int8 => Some((true, 8)),
        DataType::Int16 => Some((true, 16)),
        DataType::Int32 => Some((true, 32)),
        DataType::Int64 => Some((true, 64)),
        DataType::UInt8 => Some((false, 8)),
        DataType::UInt16 => Some((false, 16)),
        DataType::UInt32 => Some((false, 32)),
        DataType::UInt64 => Some((false, 64)),
        _ => None,
    };
    if let (DataType::Timestamp(source, None), DataType::Timestamp(target, None)) = (source, target)
    {
        return matches!(
            (source, target),
            (
                TimeUnit::Second,
                TimeUnit::Millisecond | TimeUnit::Microsecond | TimeUnit::Nanosecond
            ) | (
                TimeUnit::Millisecond,
                TimeUnit::Microsecond | TimeUnit::Nanosecond
            )
        );
    }
    match (integer(source), integer(target)) {
        (Some((true, _)), Some((true, _))) => signed_integer_cast_can_produce_null(source, target),
        (Some((true, _)), Some((false, _))) => true,
        (Some((false, source)), Some((true, target))) => target <= source,
        (Some((_, source)), Some((_, target))) => target < source,
        (_, Some(_)) if matches!(source, DataType::Float32 | DataType::Float64) => !allow,
        _ => false,
    }
}

/// Exact successful-NULL fact with the original frozen Decimal overflow policy.
/// Other cast domains retain their original existing author unchanged.
pub fn carrier_cast_can_produce_null_with_policy(
    source: &DataType,
    target: &DataType,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> bool {
    if matches!(
        source,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    ) && matches!(target, DataType::Decimal128(..))
    {
        return crate::integral_decimal128::can_produce_null(source, target, policy);
    }
    if matches!(source, DataType::Decimal128(..)) && matches!(target, DataType::Decimal128(..)) {
        return crate::decimal128_rescale::can_produce_null(policy, allow);
    }
    carrier_cast_can_produce_null(source, target, allow)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Source {
    Date32,
    Utf8,
    Boolean,
    Signed(SignedWidth),
    Unsigned(UnsignedWidth),
    F32,
    F64,
    Timestamp(TimeUnit),
}
impl Source {
    fn from_type(ty: &DataType) -> Option<Self> {
        SignedWidth::from_type(ty)
            .map(Self::Signed)
            .or_else(|| UnsignedWidth::from_type(ty).map(Self::Unsigned))
            .or(match ty {
                DataType::Date32 => Some(Self::Date32),
                DataType::Utf8 => Some(Self::Utf8),
                DataType::Boolean => Some(Self::Boolean),
                DataType::Float32 => Some(Self::F32),
                DataType::Float64 => Some(Self::F64),
                DataType::Timestamp(unit, None) => Some(Self::Timestamp(*unit)),
                _ => None,
            })
    }
    fn validate(self, array: &dyn Array) -> bool {
        match self {
            Self::Date32 => array.as_any().is::<Date32Array>(),
            Self::Utf8 => array.as_any().is::<StringArray>(),
            Self::Boolean => array.as_any().is::<BooleanArray>(),
            Self::Signed(width) => width.validate(array),
            Self::Unsigned(width) => width.validate(array),
            Self::F32 => array.as_any().is::<Float32Array>(),
            Self::F64 => array.as_any().is::<Float64Array>(),
            Self::Timestamp(unit) => match unit {
                TimeUnit::Second => array.as_any().is::<TimestampSecondArray>(),
                TimeUnit::Millisecond => array.as_any().is::<TimestampMillisecondArray>(),
                TimeUnit::Microsecond => array.as_any().is::<TimestampMicrosecondArray>(),
                TimeUnit::Nanosecond => array.as_any().is::<TimestampNanosecondArray>(),
            },
        }
    }
    const fn is_float(self) -> bool {
        matches!(self, Self::F32 | Self::F64)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Target {
    Boolean,
    Signed(SignedWidth),
    Unsigned(UnsignedWidth),
    F32,
    F64,
    Timestamp(TimeUnit),
}
impl Target {
    fn from_type(ty: &DataType) -> Option<Self> {
        SignedWidth::from_type(ty)
            .map(Self::Signed)
            .or_else(|| UnsignedWidth::from_type(ty).map(Self::Unsigned))
            .or(match ty {
                DataType::Boolean => Some(Self::Boolean),
                DataType::Float32 => Some(Self::F32),
                DataType::Float64 => Some(Self::F64),
                DataType::Timestamp(unit, None) => Some(Self::Timestamp(*unit)),
                _ => None,
            })
    }
}

/// Integer range failures are successful NULLs. Floating-to-integer failures
/// use the original ALLOW policy independently of the decimal overflow policy.
#[derive(Clone, Debug, PartialEq)]
pub enum CastRowResult {
    Null,
    Boolean(bool),
    Signed(i64),
    Unsigned(u64),
    Float32(f32),
    Float64(f64),
    Decimal128(i128),
    Timestamp(i64),
    Text(String),
    RowError(RowDataError),
}

/// Exact decimal carrier admission, independent from general carrier conversions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DecimalTextSource {
    Decimal128,
    Decimal256,
}
impl DecimalTextSource {
    fn validate(self, array: &dyn Array) -> bool {
        match self {
            Self::Decimal128 => array.as_any().is::<Decimal128Array>(),
            Self::Decimal256 => array.as_any().is::<Decimal256Array>(),
        }
    }
}

/// What a prepared cast does to a value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CastBody {
    /// Original List retag/null-child computation on an exact selected domain.
    Collection,
    IntegralDecimal {
        precision: u8,
        scale: i8,
    },
    DecimalRescale {
        precision: u8,
        scale: i8,
    },
    FloatDecimal {
        precision: u8,
        scale: i8,
    },
    TimeCalendar,
    TimeText {
        mode: crate::time_text_cast::TimeTextParseMode,
    },
    /// Original Arrow safe Binary-to-Utf8 conversion, including invalid -> NULL.
    BinaryText,
    LargeIntText,
    DateFloat {
        target: Target,
    },
    DecimalText {
        source: DecimalTextSource,
        scale: i8,
    },
    DecimalFloat {
        source: DecimalTextSource,
        scale: i8,
        target: Target,
    },
    TemporalCarrier {
        source: Source,
    },
    Text {
        source: Source,
    },
    FloatDate {
        source: Source,
    },
    Calendar {
        source: Source,
        unit: Option<TimeUnit>,
    },
    /// A primitive carrier conversion with a row operation.
    Carrier {
        source: Source,
        target: Target,
    },
    /// Same carrier and logical type; only nullability may widen. The value
    /// passes unchanged, so no row can fail or become NULL.
    Identity,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedCastRecipe {
    operation: CastOperation,
    source: FunctionValueType,
    result: FunctionValueType,
    body: CastBody,
    decimal_overflow_policy: DecimalOverflowPolicy,
    allow_throw_exception: bool,
}
impl PreparedCastRecipe {
    pub fn try_new(
        operation: CastOperation,
        source: &FunctionValueType,
        result: &FunctionValueType,
        policy: DecimalOverflowPolicy,
        allow_throw_exception: bool,
        control: &dyn PureCompileControl,
    ) -> Result<Self, CastPrepareError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let outcome = (|| {
            for ty in [source, result] {
                validate_type_observed(ty, &mut work).map_err(CastPrepareError::Kernel)?;
            }
            if operation == CastOperation::Carrier
                && source.logical_type == ValueLogicalType::Physical
                && result.logical_type == ValueLogicalType::Physical
                && source.data_type == DataType::Float64
                && let DataType::Decimal128(precision, scale) = result.data_type
            {
                // Original successful NULLs occur for nonfinite/overflow input
                // under every policy/ALLOW mode, even with a non-null source.
                if !result.nullable {
                    return Err(CastPrepareError::TypeMismatch);
                }
                work.step()?;
                return Ok(Self {
                    operation,
                    source: source.clone(),
                    result: result.clone(),
                    body: CastBody::FloatDecimal { precision, scale },
                    decimal_overflow_policy: policy,
                    allow_throw_exception,
                });
            }
            if operation == CastOperation::Carrier
                && source.logical_type == ValueLogicalType::Physical
                && result.logical_type == ValueLogicalType::Physical
                && matches!(
                    source.data_type,
                    DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
                )
                && let DataType::Decimal128(precision, scale) = result.data_type
            {
                if !crate::integral_decimal128::selected_shape_supported(
                    &source.data_type,
                    &result.data_type,
                ) {
                    return Err(CastPrepareError::Unsupported);
                }
                if (source.nullable
                    || crate::integral_decimal128::can_produce_null(
                        &source.data_type,
                        &result.data_type,
                        policy,
                    ))
                    && !result.nullable
                {
                    return Err(CastPrepareError::TypeMismatch);
                }
                work.step()?;
                return Ok(Self {
                    operation,
                    source: source.clone(),
                    result: result.clone(),
                    body: CastBody::IntegralDecimal { precision, scale },
                    decimal_overflow_policy: policy,
                    allow_throw_exception,
                });
            }
            // Decimal128's original same-metadata path enforces actual precision.
            // It is not an identity, even when only nullability differs.
            if operation == CastOperation::Carrier
                && source.logical_type == ValueLogicalType::Physical
                && result.logical_type == ValueLogicalType::Physical
                && matches!(source.data_type, DataType::Decimal128(..))
                && let DataType::Decimal128(precision, scale) = result.data_type
            {
                if (source.nullable
                    || crate::decimal128_rescale::can_produce_null(policy, allow_throw_exception))
                    && !result.nullable
                {
                    return Err(CastPrepareError::TypeMismatch);
                }
                work.step()?;
                return Ok(Self {
                    operation,
                    source: source.clone(),
                    result: result.clone(),
                    body: CastBody::DecimalRescale { precision, scale },
                    decimal_overflow_policy: policy,
                    allow_throw_exception,
                });
            }
            // A Physical carrier with no primitive conversion kernel is still
            // exactly castable to itself: the value passes unchanged and only
            // the frozen nullability may widen. Nominal identities are not a
            // Physical scalar profile and stay refused.
            let identity = operation == CastOperation::Carrier
                && source.data_type == result.data_type
                && source.logical_type == ValueLogicalType::Physical
                && result.logical_type == ValueLogicalType::Physical
                && (Source::from_type(&source.data_type).is_none()
                    || Target::from_type(&result.data_type).is_none());
            work.step()?;
            if identity {
                if source.nullable && !result.nullable {
                    return Err(CastPrepareError::TypeMismatch);
                }
                return Ok(Self {
                    operation,
                    source: source.clone(),
                    result: result.clone(),
                    body: CastBody::Identity,
                    decimal_overflow_policy: policy,
                    allow_throw_exception,
                });
            }
            let physical = source.logical_type == ValueLogicalType::Physical
                && result.logical_type == ValueLogicalType::Physical;
            work.step()?;
            if operation == CastOperation::Carrier
                && crate::list_cast_selected::profile(source, result)
            {
                if source.nullable && !result.nullable {
                    return Err(CastPrepareError::TypeMismatch);
                }
                return Ok(Self {
                    operation,
                    source: source.clone(),
                    result: result.clone(),
                    body: CastBody::Collection,
                    decimal_overflow_policy: policy,
                    allow_throw_exception,
                });
            }
            if physical
                && matches!(
                    source.data_type,
                    DataType::Date32 | DataType::Timestamp(_, _)
                )
                && result.data_type == DataType::Time64(TimeUnit::Microsecond)
            {
                if (source.nullable
                    || crate::time_calendar_cast::can_produce_null(&source.data_type))
                    && !result.nullable
                {
                    return Err(CastPrepareError::TypeMismatch);
                }
                return Ok(Self {
                    operation,
                    source: source.clone(),
                    result: result.clone(),
                    body: CastBody::TimeCalendar,
                    decimal_overflow_policy: policy,
                    allow_throw_exception,
                });
            }
            if physical
                && matches!(
                    source.data_type,
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                )
                && result.data_type == DataType::Time64(TimeUnit::Microsecond)
            {
                if !result.nullable {
                    return Err(CastPrepareError::TypeMismatch);
                }
                let mode = match operation {
                    CastOperation::Carrier | CastOperation::Time => {
                        crate::time_text_cast::TimeTextParseMode::Duration
                    }
                    CastOperation::TimeFromDatetime => {
                        crate::time_text_cast::TimeTextParseMode::Datetime
                    }
                };
                return Ok(Self {
                    operation,
                    source: source.clone(),
                    result: result.clone(),
                    body: CastBody::TimeText { mode },
                    decimal_overflow_policy: policy,
                    allow_throw_exception,
                });
            }
            if operation != CastOperation::Carrier || !physical {
                return Err(CastPrepareError::Unsupported);
            }
            if source.data_type == DataType::Binary && result.data_type == DataType::Utf8 {
                work.step()?;
                if !result.nullable {
                    return Err(CastPrepareError::TypeMismatch);
                }
                return Ok(Self {
                    operation,
                    source: source.clone(),
                    result: result.clone(),
                    body: CastBody::BinaryText,
                    decimal_overflow_policy: policy,
                    allow_throw_exception,
                });
            }
            if result.data_type == DataType::Utf8
                && matches!(
                    source.data_type,
                    DataType::Decimal128(..) | DataType::Decimal256(..)
                )
            {
                let decimal = match source.data_type {
                    DataType::Decimal128(_, scale) => Some((DecimalTextSource::Decimal128, scale)),
                    DataType::Decimal256(_, scale) => Some((DecimalTextSource::Decimal256, scale)),
                    _ => None,
                };
                work.step()?;
                if let Some((decimal, scale)) = decimal {
                    if source.nullable && !result.nullable {
                        return Err(CastPrepareError::TypeMismatch);
                    }
                    return Ok(Self {
                        operation,
                        source: source.clone(),
                        result: result.clone(),
                        body: CastBody::DecimalText {
                            source: decimal,
                            scale,
                        },
                        decimal_overflow_policy: policy,
                        allow_throw_exception,
                    });
                }
            }
            if matches!(result.data_type, DataType::Float32 | DataType::Float64)
                && matches!(
                    source.data_type,
                    DataType::Decimal128(..) | DataType::Decimal256(..)
                )
            {
                let decimal = match source.data_type {
                    DataType::Decimal128(_, scale) => Some((DecimalTextSource::Decimal128, scale)),
                    DataType::Decimal256(_, scale) => Some((DecimalTextSource::Decimal256, scale)),
                    _ => None,
                };
                work.step()?;
                if let Some((decimal, scale)) = decimal {
                    if !result.nullable
                        && (source.nullable
                            || carrier_cast_can_produce_null(
                                &source.data_type,
                                &result.data_type,
                                allow_throw_exception,
                            ))
                    {
                        return Err(CastPrepareError::TypeMismatch);
                    }
                    return Ok(Self {
                        operation,
                        source: source.clone(),
                        result: result.clone(),
                        body: CastBody::DecimalFloat {
                            source: decimal,
                            scale,
                            target: if result.data_type == DataType::Float32 {
                                Target::F32
                            } else {
                                Target::F64
                            },
                        },
                        decimal_overflow_policy: policy,
                        allow_throw_exception,
                    });
                }
            }
            if source.data_type == DataType::FixedSizeBinary(16)
                && result.data_type == DataType::Utf8
            {
                work.step()?;
                if source.nullable && !result.nullable {
                    return Err(CastPrepareError::TypeMismatch);
                }
                return Ok(Self {
                    operation,
                    source: source.clone(),
                    result: result.clone(),
                    body: CastBody::LargeIntText,
                    decimal_overflow_policy: policy,
                    allow_throw_exception,
                });
            }
            let source_kind =
                Source::from_type(&source.data_type).ok_or(CastPrepareError::Unsupported)?;
            if source_kind == Source::Date32
                && matches!(result.data_type, DataType::Float32 | DataType::Float64)
            {
                if source.nullable && !result.nullable {
                    return Err(CastPrepareError::TypeMismatch);
                }
                let target = if result.data_type == DataType::Float32 {
                    Target::F32
                } else {
                    Target::F64
                };
                return Ok(Self {
                    operation,
                    source: source.clone(),
                    result: result.clone(),
                    body: CastBody::DateFloat { target },
                    decimal_overflow_policy: policy,
                    allow_throw_exception,
                });
            }
            if crate::temporal_carrier::supports(&source.data_type, &result.data_type) {
                if source.nullable && !result.nullable {
                    return Err(CastPrepareError::TypeMismatch);
                }
                return Ok(Self {
                    operation,
                    source: source.clone(),
                    result: result.clone(),
                    body: CastBody::TemporalCarrier {
                        source: source_kind,
                    },
                    decimal_overflow_policy: policy,
                    allow_throw_exception,
                });
            }
            if result.data_type == DataType::Utf8 && source_kind != Source::Utf8 {
                if source.nullable && !result.nullable {
                    return Err(CastPrepareError::TypeMismatch);
                }
                return Ok(Self {
                    operation,
                    source: source.clone(),
                    result: result.clone(),
                    body: CastBody::Text {
                        source: source_kind,
                    },
                    decimal_overflow_policy: policy,
                    allow_throw_exception,
                });
            }
            if source_kind.is_float() && result.data_type == DataType::Date32 {
                if source.nullable && !result.nullable {
                    return Err(CastPrepareError::TypeMismatch);
                }
                return Ok(Self {
                    operation,
                    source: source.clone(),
                    result: result.clone(),
                    body: CastBody::FloatDate {
                        source: source_kind,
                    },
                    decimal_overflow_policy: policy,
                    allow_throw_exception,
                });
            }
            if !matches!(source_kind, Source::Timestamp(_))
                && matches!(
                    result.data_type,
                    DataType::Date32 | DataType::Timestamp(_, None)
                )
            {
                if !result.nullable {
                    return Err(CastPrepareError::TypeMismatch);
                }
                let unit = match result.data_type {
                    DataType::Timestamp(unit, None) => Some(unit),
                    _ => None,
                };
                return Ok(Self {
                    operation,
                    source: source.clone(),
                    result: result.clone(),
                    body: CastBody::Calendar {
                        source: source_kind,
                        unit,
                    },
                    decimal_overflow_policy: policy,
                    allow_throw_exception,
                });
            }
            let target =
                Target::from_type(&result.data_type).ok_or(CastPrepareError::Unsupported)?;
            if source_kind == Source::Utf8 && !matches!(target, Target::Signed(_) | Target::Boolean)
            {
                return Err(CastPrepareError::Unsupported);
            }
            let source_timestamp = matches!(source_kind, Source::Timestamp(_));
            let target_timestamp = matches!(target, Target::Timestamp(_));
            work.step()?;
            if source_timestamp != target_timestamp {
                return Err(CastPrepareError::Unsupported);
            }
            let successful_null = carrier_cast_can_produce_null(
                &source.data_type,
                &result.data_type,
                allow_throw_exception,
            );
            let valid_nullable = result.nullable || (!source.nullable && !successful_null);
            work.step()?;
            if !valid_nullable {
                return Err(CastPrepareError::TypeMismatch);
            }
            // Admitted timestamp zones are None; scalar types retain no owned graph.
            let recipe = Self {
                operation,
                source: source.clone(),
                result: result.clone(),
                body: CastBody::Carrier {
                    source: source_kind,
                    target,
                },
                decimal_overflow_policy: policy,
                allow_throw_exception,
            };
            work.step()?;
            Ok(recipe)
        })();
        if outcome
            .as_ref()
            .err()
            .is_some_and(|error| error.control_error().is_some())
        {
            return outcome;
        }
        work.finish()?;
        outcome
    }
    pub fn operation(&self) -> CastOperation {
        self.operation
    }
    pub fn source_type(&self) -> &FunctionValueType {
        &self.source
    }
    pub fn result_type(&self) -> &FunctionValueType {
        &self.result
    }
    pub fn decimal_overflow_policy(&self) -> DecimalOverflowPolicy {
        self.decimal_overflow_policy
    }
    pub fn policy(&self) -> DecimalOverflowPolicy {
        self.decimal_overflow_policy
    }
    pub fn allow_throw_exception(&self) -> bool {
        self.allow_throw_exception
    }
    /// An identity cast has no row operation: its value passes unchanged.
    pub fn is_identity(&self) -> bool {
        self.body == CastBody::Identity
    }
    /// The selected controller delegates this exact body as one invocation.
    pub fn is_collection(&self) -> bool {
        self.body == CastBody::Collection
    }
    pub fn evaluate_collection<'a>(
        &self,
        argument: EvaluatedArgument<'_>,
        selection: crate::Selection<'a>,
        inherited: &[RowDataError],
        control: &dyn KernelEvaluationControl,
    ) -> Result<crate::SelectedValues<'a>, KernelFailure> {
        if !self.is_collection() {
            return Err(invalid(
                "collection invocation requires a prepared List CAST",
            ));
        }
        crate::list_cast_selected::evaluate(
            &self.source,
            &self.result,
            argument,
            selection,
            inherited,
            control,
        )
    }
    pub fn own_effects(&self, context: ExpressionEffectContext) -> ScopedExpressionEffects {
        let may_raise_row_error = match self.body {
            CastBody::IntegralDecimal { .. } => crate::integral_decimal128::may_raise(
                &self.source.data_type,
                &self.result.data_type,
                self.decimal_overflow_policy,
            ),
            CastBody::DecimalRescale { scale, .. } => {
                let DataType::Decimal128(_, source_scale) = self.source.data_type else {
                    unreachable!("prepared exact Decimal128 source")
                };
                crate::decimal128_rescale::may_raise(
                    source_scale,
                    scale,
                    self.decimal_overflow_policy,
                    self.allow_throw_exception,
                )
            }
            // Preserve original signed MIN abs and -128 negation panics; static
            // factor errors also retain their real selected row attribution.
            CastBody::FloatDecimal { .. } => true,
            // Raw Date32 admits invalid days and original unchecked arithmetic.
            // Preserve its data error and overflow panic rather than promising never-fails.
            CastBody::DateFloat { .. } => true,
            // Conservatively retain failure awareness for the original i128 MIN
            // positive-scale abs bug; do not translate its panic into a row error.
            CastBody::DecimalText {
                source: DecimalTextSource::Decimal128,
                scale,
            } => scale > 0,
            CastBody::DecimalText {
                source: DecimalTextSource::Decimal256,
                ..
            } => false,
            // The original Decimal256 -scale expression panics for i8::MIN
            // in checked builds. Retain failure awareness without converting it.
            CastBody::DecimalFloat { source, scale, .. } => {
                source == DecimalTextSource::Decimal256 && scale == i8::MIN
            }
            CastBody::TemporalCarrier { .. } => {
                matches!(self.result.data_type, DataType::Date32 | DataType::Utf8)
            }
            CastBody::FloatDate { .. } => true,
            CastBody::Calendar { unit, .. } => unit == Some(TimeUnit::Nanosecond),
            CastBody::Collection
            | CastBody::TimeCalendar
            | CastBody::TimeText { .. }
            | CastBody::BinaryText
            | CastBody::LargeIntText
            | CastBody::Identity
            | CastBody::Text { .. } => false,
            CastBody::Carrier { source, target } => {
                (source.is_float()
                    && matches!(target, Target::Signed(_) | Target::Unsigned(_))
                    && self.allow_throw_exception)
                    || matches!(
                        (source, target),
                        (
                            Source::Timestamp(TimeUnit::Microsecond),
                            Target::Timestamp(TimeUnit::Nanosecond)
                        )
                    )
            }
        };
        ScopedExpressionEffects::primitive(
            context,
            ExpressionEffects {
                may_raise_row_error,
                ..ExpressionEffects::PURE_VALUE
            },
        )
    }
    /// The host excludes inherited errors before this selected-row operation.
    /// Both the actual address and concrete scalar carrier are checked before NULL.
    pub fn evaluate_row(
        &self,
        argument: EvaluatedArgument<'_>,
        ordinal: usize,
        logical_row: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<CastRowResult, KernelFailure> {
        control.checkpoint(0)?;
        let mut work = EvaluationCheckpoints::new(control);
        let outcome = (|| {
            if self.is_collection() {
                return Err(invalid("List CAST requires its actual selected invocation"));
            }
            if let CastBody::FloatDecimal { precision, scale } = self.body {
                let row = self.checked_row_with_shape(
                    argument,
                    ordinal,
                    logical_row,
                    &mut work,
                    |array| array.as_any().is::<Float64Array>(),
                )?;
                // Static factors precede NULL masking exactly as the original
                // Types array conversion. No value is validated during prepare.
                work.flush()?;
                let factors = match crate::float_decimal128::Factors::try_new(precision, scale) {
                    Ok(factors) => factors,
                    Err(recipe) => {
                        let message = format!(
                            "CAST failed: from {:?} to {:?}: {recipe}",
                            self.source.data_type, self.result.data_type,
                        );
                        return Ok(CastRowResult::RowError(RowDataError::new(
                            ordinal, &message,
                        )));
                    }
                };
                work.flush()?;
                if logical_is_null(argument.array().as_ref(), row, 1, &mut work)? {
                    return if self.source.nullable {
                        Ok(CastRowResult::Null)
                    } else {
                        Err(invalid(
                            "non-null float Decimal cast argument contains a selected NULL",
                        ))
                    };
                }
                let value = argument
                    .array()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| internal("float Decimal source has a foreign carrier"))?
                    .value(row);
                work.step()?;
                return Ok(
                    match factors.expr_value(value, precision, self.decimal_overflow_policy) {
                        crate::float_decimal128::ExprValue::Value(value) => {
                            CastRowResult::Decimal128(value)
                        }
                        crate::float_decimal128::ExprValue::Null => CastRowResult::Null,
                        crate::float_decimal128::ExprValue::Overflow => {
                            CastRowResult::RowError(RowDataError::new(
                                ordinal,
                                "Expr evaluate meet error: The numeric type cast involving decimal overflows",
                            ))
                        }
                    },
                );
            }
            if let CastBody::IntegralDecimal { precision, scale } = self.body {
                let source_kind = Source::from_type(&self.source.data_type).ok_or_else(|| {
                    internal("integral Decimal source lost its exact signed carrier")
                })?;
                let row =
                    self.checked_row(source_kind, argument, ordinal, logical_row, &mut work)?;
                if logical_is_null(argument.array().as_ref(), row, 1, &mut work)? {
                    return if self.source.nullable {
                        Ok(CastRowResult::Null)
                    } else {
                        Err(invalid(
                            "non-null integral Decimal cast argument contains a selected NULL",
                        ))
                    };
                }
                work.flush()?;
                let selected = argument.array().slice(row, 1);
                work.flush()?;
                let mut observe = |event| match event {
                    crate::decimal128_rescale::DecimalRescaleObservation::Step => work.step(),
                    crate::decimal128_rescale::DecimalRescaleObservation::OpaqueBoundary => {
                        work.flush()
                    }
                };
                let output = match crate::integral_decimal128::evaluate_observed(
                    &selected,
                    precision,
                    scale,
                    self.decimal_overflow_policy,
                    self.allow_throw_exception,
                    &mut observe,
                ) {
                    Ok(output) => output,
                    Err(crate::decimal128_rescale::DecimalRescaleError::Host(cause)) => {
                        return Err(cause);
                    }
                    Err(crate::decimal128_rescale::DecimalRescaleError::Data(message)) => {
                        return Ok(CastRowResult::RowError(RowDataError::new(
                            ordinal, &message,
                        )));
                    }
                };
                work.flush()?;
                let output = output
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| {
                        internal("integral Decimal core returned a foreign frozen carrier")
                    })?;
                return Ok(if output.is_null(0) {
                    CastRowResult::Null
                } else {
                    CastRowResult::Decimal128(output.value(0))
                });
            }
            if let CastBody::DecimalRescale { precision, scale } = self.body {
                let row = self.checked_row_with_shape(
                    argument,
                    ordinal,
                    logical_row,
                    &mut work,
                    |array| array.as_any().is::<Decimal128Array>(),
                )?;
                if logical_is_null(argument.array().as_ref(), row, 1, &mut work)? {
                    return if self.source.nullable {
                        Ok(CastRowResult::Null)
                    } else {
                        Err(invalid(
                            "non-null Decimal128 cast argument contains a selected NULL",
                        ))
                    };
                }
                work.flush()?;
                let selected = argument.array().slice(row, 1);
                work.flush()?;
                let mut observe = |event| match event {
                    crate::decimal128_rescale::DecimalRescaleObservation::Step => work.step(),
                    crate::decimal128_rescale::DecimalRescaleObservation::OpaqueBoundary => {
                        work.flush()
                    }
                };
                let output = match crate::decimal128_rescale::evaluate_observed(
                    &selected,
                    precision,
                    scale,
                    self.decimal_overflow_policy,
                    self.allow_throw_exception,
                    &mut observe,
                ) {
                    Ok(output) => output,
                    Err(crate::decimal128_rescale::DecimalRescaleError::Host(cause)) => {
                        return Err(cause);
                    }
                    Err(crate::decimal128_rescale::DecimalRescaleError::Data(message)) => {
                        return Ok(CastRowResult::RowError(RowDataError::new(
                            ordinal, &message,
                        )));
                    }
                };
                work.flush()?;
                let output = output
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| {
                        internal("Decimal128 rescale core returned a foreign frozen carrier")
                    })?;
                return Ok(if output.is_null(0) {
                    CastRowResult::Null
                } else {
                    CastRowResult::Decimal128(output.value(0))
                });
            }
            if self.body == CastBody::TimeCalendar {
                let row = self.checked_row_with_shape(
                    argument,
                    ordinal,
                    logical_row,
                    &mut work,
                    |array| match self.source.data_type {
                        DataType::Date32 => array.as_any().is::<Date32Array>(),
                        DataType::Timestamp(TimeUnit::Second, _) => {
                            array.as_any().is::<TimestampSecondArray>()
                        }
                        DataType::Timestamp(TimeUnit::Millisecond, _) => {
                            array.as_any().is::<TimestampMillisecondArray>()
                        }
                        DataType::Timestamp(TimeUnit::Microsecond, _) => {
                            array.as_any().is::<TimestampMicrosecondArray>()
                        }
                        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                            array.as_any().is::<TimestampNanosecondArray>()
                        }
                        _ => false,
                    },
                )?;
                if logical_is_null(argument.array().as_ref(), row, 1, &mut work)? {
                    return if self.source.nullable {
                        Ok(CastRowResult::Null)
                    } else {
                        Err(invalid(
                            "non-null calendar TIME argument contains a selected NULL",
                        ))
                    };
                }
                work.flush()?;
                let selected = argument.array().slice(row, 1);
                work.flush()?;
                let mut observe = |event| match event {
                    crate::time_text_cast::TimeTextObservation::Step => work.step(),
                    crate::time_text_cast::TimeTextObservation::OpaqueBoundary => work.flush(),
                };
                let output = crate::time_calendar_cast::evaluate_arrays_observed(
                    &selected,
                    &self.result.data_type,
                    Some(&mut observe),
                )?;
                let output = match output {
                    Ok(output) => output,
                    Err(message) => {
                        return Ok(CastRowResult::RowError(RowDataError::new(
                            ordinal, &message,
                        )));
                    }
                };
                work.flush()?;
                let output = output
                    .as_any()
                    .downcast_ref::<arrow_array::Time64MicrosecondArray>()
                    .ok_or_else(|| {
                        internal("calendar TIME core returned a different frozen carrier")
                    })?;
                return Ok(if output.is_null(0) {
                    CastRowResult::Null
                } else {
                    CastRowResult::Signed(output.value(0))
                });
            }
            if let CastBody::TimeText { mode } = self.body {
                let row = self.checked_row_with_shape(
                    argument,
                    ordinal,
                    logical_row,
                    &mut work,
                    |array| match self.source.data_type {
                        DataType::Utf8 => array.as_any().is::<StringArray>(),
                        DataType::LargeUtf8 => array.as_any().is::<arrow_array::LargeStringArray>(),
                        DataType::Utf8View => array.as_any().is::<arrow_array::StringViewArray>(),
                        _ => false,
                    },
                )?;
                if logical_is_null(argument.array().as_ref(), row, 1, &mut work)? {
                    return if self.source.nullable {
                        Ok(CastRowResult::Null)
                    } else {
                        Err(invalid(
                            "non-null TIME cast argument contains a selected NULL",
                        ))
                    };
                }
                work.flush()?;
                let selected = argument.array().slice(row, 1);
                work.flush()?;
                let mut observe = |event| match event {
                    crate::time_text_cast::TimeTextObservation::Step => work.step(),
                    crate::time_text_cast::TimeTextObservation::OpaqueBoundary => work.flush(),
                };
                let output = crate::time_text_cast::evaluate_arrays_observed(
                    &selected,
                    &self.result.data_type,
                    mode,
                    Some(&mut observe),
                )?;
                let output = match output {
                    Ok(output) => output,
                    Err(message) => {
                        return Ok(CastRowResult::RowError(RowDataError::new(
                            ordinal, &message,
                        )));
                    }
                };
                work.flush()?;
                let output = output
                    .as_any()
                    .downcast_ref::<arrow_array::Time64MicrosecondArray>()
                    .ok_or_else(|| internal("TIME core returned a different frozen carrier"))?;
                return Ok(if output.is_null(0) {
                    CastRowResult::Null
                } else {
                    CastRowResult::Signed(output.value(0))
                });
            }
            if self.body == CastBody::BinaryText {
                let row = self.checked_row_with_shape(
                    argument,
                    ordinal,
                    logical_row,
                    &mut work,
                    |array| array.as_any().is::<BinaryArray>(),
                )?;
                if logical_is_null(argument.array().as_ref(), row, 1, &mut work)? {
                    return if self.source.nullable {
                        Ok(CastRowResult::Null)
                    } else {
                        Err(invalid("non-null cast argument contains a selected NULL"))
                    };
                }
                let input = argument
                    .array()
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| internal("checked binary text has a foreign carrier"))?;
                // Observe the actual selected byte extent without decoding it twice.
                // The Arrow validation and owned result copy remain opaque work.
                for _ in input.value(row) {
                    work.step()?;
                }
                work.flush()?;
                let text = crate::binary_text::value_text(argument.array(), row)
                    .map_err(|error| internal(&error))?;
                work.flush()?;
                return Ok(text.map(CastRowResult::Text).unwrap_or(CastRowResult::Null));
            }
            if self.body == CastBody::LargeIntText {
                let row = self.checked_row_with_shape(
                    argument,
                    ordinal,
                    logical_row,
                    &mut work,
                    |array| array.as_any().is::<FixedSizeBinaryArray>(),
                )?;
                if logical_is_null(argument.array().as_ref(), row, 1, &mut work)? {
                    return if self.source.nullable {
                        Ok(CastRowResult::Null)
                    } else {
                        Err(invalid("non-null cast argument contains a selected NULL"))
                    };
                }
                let source = argument
                    .array()
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .ok_or_else(|| internal("checked LARGEINT text has a foreign carrier"))?;
                work.flush()?;
                let text = crate::largeint_text::value_text(source, row)
                    .map_err(|error| internal(&error))?;
                work.flush()?;
                return Ok(CastRowResult::Text(text));
            }
            if let CastBody::DecimalText { source, scale } = self.body {
                let row = self.checked_row_with_shape(
                    argument,
                    ordinal,
                    logical_row,
                    &mut work,
                    |array| source.validate(array),
                )?;
                if logical_is_null(argument.array().as_ref(), row, 1, &mut work)? {
                    return if self.source.nullable {
                        Ok(CastRowResult::Null)
                    } else {
                        Err(invalid("non-null cast argument contains a selected NULL"))
                    };
                }
                work.flush()?;
                let text = match source {
                    DecimalTextSource::Decimal128 => {
                        crate::decimal_text::format_decimal_with_scale(
                            argument
                                .array()
                                .as_any()
                                .downcast_ref::<Decimal128Array>()
                                .ok_or_else(|| {
                                    internal("checked decimal text has a foreign carrier")
                                })?
                                .value(row),
                            scale,
                        )
                    }
                    DecimalTextSource::Decimal256 => {
                        crate::decimal_text::format_decimal256_with_scale(
                            argument
                                .array()
                                .as_any()
                                .downcast_ref::<Decimal256Array>()
                                .ok_or_else(|| {
                                    internal("checked decimal text has a foreign carrier")
                                })?
                                .value(row),
                            scale,
                        )
                    }
                };
                work.flush()?;
                return Ok(CastRowResult::Text(text));
            }
            if let CastBody::DecimalFloat {
                source,
                scale,
                target,
            } = self.body
            {
                let row = self.checked_row_with_shape(
                    argument,
                    ordinal,
                    logical_row,
                    &mut work,
                    |array| source.validate(array),
                )?;
                if logical_is_null(argument.array().as_ref(), row, 1, &mut work)? {
                    return if self.source.nullable {
                        Ok(CastRowResult::Null)
                    } else {
                        Err(invalid("non-null cast argument contains a selected NULL"))
                    };
                }
                work.flush()?;
                let value = match target {
                    Target::F64 => CastRowResult::Float64(match source {
                        DecimalTextSource::Decimal128 => {
                            crate::decimal_float_cast::decimal128_to_f64(
                                argument
                                    .array()
                                    .as_any()
                                    .downcast_ref::<Decimal128Array>()
                                    .ok_or_else(|| {
                                        internal("checked decimal float has a foreign carrier")
                                    })?
                                    .value(row),
                                scale,
                            )
                        }
                        DecimalTextSource::Decimal256 => {
                            crate::decimal_float_cast::decimal256_to_f64(
                                argument
                                    .array()
                                    .as_any()
                                    .downcast_ref::<Decimal256Array>()
                                    .ok_or_else(|| {
                                        internal("checked decimal float has a foreign carrier")
                                    })?
                                    .value(row),
                                scale,
                            )
                        }
                    }),
                    Target::F32 => match source {
                        DecimalTextSource::Decimal128 => {
                            let narrowed = crate::decimal_float_cast::decimal128_to_f32(
                                argument
                                    .array()
                                    .as_any()
                                    .downcast_ref::<Decimal128Array>()
                                    .ok_or_else(|| {
                                        internal("checked decimal float has a foreign carrier")
                                    })?
                                    .value(row),
                                scale,
                            );
                            crate::decimal_float_cast::finite_f32_value(narrowed)
                                .map(CastRowResult::Float32)
                                .unwrap_or(CastRowResult::Null)
                        }
                        DecimalTextSource::Decimal256 => {
                            CastRowResult::Float32(crate::decimal_float_cast::decimal256_to_f32(
                                argument
                                    .array()
                                    .as_any()
                                    .downcast_ref::<Decimal256Array>()
                                    .ok_or_else(|| {
                                        internal("checked decimal float has a foreign carrier")
                                    })?
                                    .value(row),
                                scale,
                            ))
                        }
                    },
                    _ => return Err(internal("decimal float has a foreign frozen target")),
                };
                work.flush()?;
                return Ok(value);
            }
            if let CastBody::DateFloat { target } = self.body {
                let row =
                    self.checked_row(Source::Date32, argument, ordinal, logical_row, &mut work)?;
                if logical_is_null(argument.array().as_ref(), row, 1, &mut work)? {
                    return if self.source.nullable {
                        Ok(CastRowResult::Null)
                    } else {
                        Err(invalid("non-null cast argument contains a selected NULL"))
                    };
                }
                let days = argument
                    .array()
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .ok_or_else(|| internal("checked date float has a foreign carrier"))?
                    .value(row);
                work.flush()?;
                let converted = match target {
                    Target::F32 => {
                        crate::date_float_cast::value_f32(days).map(CastRowResult::Float32)
                    }
                    Target::F64 => {
                        crate::date_float_cast::value_f64(days).map(CastRowResult::Float64)
                    }
                    _ => return Err(internal("date float has a foreign frozen target")),
                };
                work.flush()?;
                return Ok(match converted {
                    Ok(value) => value,
                    Err(message) => CastRowResult::RowError(RowDataError::new(
                        ordinal,
                        &format!(
                            "CAST failed: from {:?} to {:?}: {message}",
                            self.source.data_type, self.result.data_type
                        ),
                    )),
                });
            }
            if let CastBody::TemporalCarrier { source } = self.body {
                let row = self.checked_row(source, argument, ordinal, logical_row, &mut work)?;
                let is_null = logical_is_null(argument.array().as_ref(), row, 1, &mut work)?;
                if is_null && !self.source.nullable {
                    return Err(invalid("non-null cast argument contains a selected NULL"));
                }
                // The original Arrow body controls NULL visitation. Do not mask
                // Date32's hidden-payload multiplication before invoking it.
                work.flush()?;
                let selected = argument.array().slice(row, 1);
                let converted =
                    crate::temporal_carrier::cast(selected.as_ref(), &self.result.data_type);
                work.flush()?;
                let converted = match converted {
                    Ok(array) => array,
                    Err(message) => {
                        return Ok(CastRowResult::RowError(RowDataError::new(
                            ordinal, &message,
                        )));
                    }
                };
                if converted.is_null(0) {
                    return if self.result.nullable {
                        Ok(CastRowResult::Null)
                    } else {
                        Err(internal("temporal cast NULL contradicts its exact result"))
                    };
                }
                macro_rules! value {
                    ($array:ty) => {
                        converted
                            .as_any()
                            .downcast_ref::<$array>()
                            .ok_or_else(|| internal("temporal cast returned a foreign carrier"))?
                            .value(0)
                    };
                }
                return Ok(match &self.result.data_type {
                    DataType::Date32 => CastRowResult::Signed(i64::from(value!(Date32Array))),
                    DataType::Utf8 => CastRowResult::Text(value!(StringArray).to_owned()),
                    DataType::Timestamp(TimeUnit::Second, None) => {
                        CastRowResult::Timestamp(value!(TimestampSecondArray))
                    }
                    DataType::Timestamp(TimeUnit::Millisecond, None) => {
                        CastRowResult::Timestamp(value!(TimestampMillisecondArray))
                    }
                    DataType::Timestamp(TimeUnit::Microsecond, None) => {
                        CastRowResult::Timestamp(value!(TimestampMicrosecondArray))
                    }
                    DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                        CastRowResult::Timestamp(value!(TimestampNanosecondArray))
                    }
                    _ => return Err(internal("temporal cast has a foreign frozen result")),
                });
            }
            if let CastBody::Text { source } = self.body {
                let row = self.checked_row(source, argument, ordinal, logical_row, &mut work)?;
                if logical_is_null(argument.array().as_ref(), row, 1, &mut work)? {
                    return if self.source.nullable {
                        Ok(CastRowResult::Null)
                    } else {
                        Err(invalid("non-null cast argument contains a selected NULL"))
                    };
                }
                work.flush()?;
                let text = crate::carrier_text::render(argument.array().as_ref(), row)
                    .map_err(|_| internal("checked text cast has a foreign carrier"))?;
                work.flush()?;
                return Ok(CastRowResult::Text(text));
            }
            if let CastBody::FloatDate { source } = self.body {
                let row = self.checked_row(source, argument, ordinal, logical_row, &mut work)?;
                if logical_is_null(argument.array().as_ref(), row, 1, &mut work)? {
                    return if self.source.nullable {
                        Ok(CastRowResult::Null)
                    } else {
                        Err(invalid("non-null cast argument contains a selected NULL"))
                    };
                }
                work.flush()?;
                let converted = match source {
                    Source::F32 => crate::float_date_cast::value_f32(
                        argument
                            .array()
                            .as_any()
                            .downcast_ref::<Float32Array>()
                            .ok_or_else(|| internal("checked float DATE has a foreign carrier"))?
                            .value(row),
                    ),
                    Source::F64 => crate::float_date_cast::value_f64(
                        argument
                            .array()
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .ok_or_else(|| internal("checked float DATE has a foreign carrier"))?
                            .value(row),
                    ),
                    _ => return Err(internal("float DATE has a foreign frozen source")),
                };
                work.flush()?;
                return Ok(match converted {
                    Ok(value) => CastRowResult::Signed(i64::from(value)),
                    Err(message) => CastRowResult::RowError(RowDataError::new(
                        ordinal,
                        &format!(
                            "CAST failed: from {:?} to Date32: {message}",
                            self.source.data_type
                        ),
                    )),
                });
            }
            if let CastBody::Calendar { source, unit } = self.body {
                let row = self.checked_row(source, argument, ordinal, logical_row, &mut work)?;
                if logical_is_null(argument.array().as_ref(), row, 1, &mut work)? {
                    return if self.source.nullable {
                        Ok(CastRowResult::Null)
                    } else {
                        Err(invalid("non-null cast argument contains a selected NULL"))
                    };
                }
                return calendar::evaluate(
                    source,
                    argument.array().as_ref(),
                    row,
                    ordinal,
                    unit,
                    &mut work,
                );
            }
            let CastBody::Carrier {
                source: source_kind,
                target: carrier_target,
            } = self.body
            else {
                return Err(invalid(
                    "an identity cast has no row operation; its value passes unchanged",
                ));
            };
            let row = self.checked_row(source_kind, argument, ordinal, logical_row, &mut work)?;
            if logical_is_null(argument.array().as_ref(), row, 1, &mut work)? {
                return if self.source.nullable {
                    Ok(CastRowResult::Null)
                } else {
                    Err(invalid("non-null cast argument contains a selected NULL"))
                };
            }
            if source_kind == Source::Utf8 {
                let text = argument
                    .array()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| internal("cast carrier has a foreign array implementation"))?
                    .value(row);
                let value = match carrier_target {
                    Target::Signed(width) => {
                        let parsed = crate::builtin::round_cast_text::parse_i64(text, &mut work)?;
                        parsed
                            .and_then(|number| match width {
                                SignedWidth::I8 => i8::try_from(number).ok().map(i64::from),
                                SignedWidth::I16 => i16::try_from(number).ok().map(i64::from),
                                SignedWidth::I32 => i32::try_from(number).ok().map(i64::from),
                                SignedWidth::I64 => Some(number),
                            })
                            .map(CastRowResult::Signed)
                    }
                    Target::Boolean => {
                        // Observe trimming before the borrowed standard-library operation.
                        for _ in text.chars() {
                            work.step()?;
                        }
                        let trimmed = text.trim();
                        let integer =
                            crate::builtin::round_cast_text::parse_i64(trimmed, &mut work)?
                                .and_then(|value| i32::try_from(value).ok());
                        integer
                            .map(|value| value != 0)
                            .or_else(|| {
                                if trimmed.eq_ignore_ascii_case("true") {
                                    Some(true)
                                } else if trimmed.eq_ignore_ascii_case("false") {
                                    Some(false)
                                } else {
                                    None
                                }
                            })
                            .map(CastRowResult::Boolean)
                    }
                    _ => return Err(internal("text cast contains a foreign frozen target")),
                };
                work.step()?;
                return Ok(value.unwrap_or(CastRowResult::Null));
            }
            if let Source::Timestamp(unit) = source_kind {
                let Target::Timestamp(target) = carrier_target else {
                    return Err(internal("timestamp cast contains a foreign frozen target"));
                };
                macro_rules! read_timestamp {
                    ($array:ty) => {
                        argument
                            .array()
                            .as_any()
                            .downcast_ref::<$array>()
                            .ok_or_else(|| {
                                internal("cast carrier has a foreign array implementation")
                            })?
                            .value(row)
                    };
                }
                let value = match unit {
                    TimeUnit::Second => read_timestamp!(TimestampSecondArray),
                    TimeUnit::Millisecond => read_timestamp!(TimestampMillisecondArray),
                    TimeUnit::Microsecond => read_timestamp!(TimestampMicrosecondArray),
                    TimeUnit::Nanosecond => read_timestamp!(TimestampNanosecondArray),
                };
                work.step()?;
                let factor = |unit| match unit {
                    TimeUnit::Second => 1_i64,
                    TimeUnit::Millisecond => 1_000,
                    TimeUnit::Microsecond => 1_000_000,
                    TimeUnit::Nanosecond => 1_000_000_000,
                };
                let source_factor = factor(unit);
                let target_factor = factor(target);
                // This exact scalar arithmetic is the original Arrow unit conversion.
                // Narrowing divides toward zero; it is not calendar decomposition.
                let converted = if source_factor >= target_factor {
                    Some(value / (source_factor / target_factor))
                } else {
                    value.checked_mul(target_factor / source_factor)
                };
                work.step()?;
                return match converted {
                    Some(value) => Ok(CastRowResult::Timestamp(value)),
                    None if unit == TimeUnit::Microsecond && target == TimeUnit::Nanosecond => {
                        work.flush()?;
                        // Formatting and diagnostic ownership are finite opaque work.
                        // The original special error precedes ALLOW handling.
                        let message = format!(
                            "CAST failed: from {:?} to {:?}: CAST timestamp microsecond->nanosecond overflow: value {} cannot be represented as nanoseconds in i64",
                            self.source.data_type, self.result.data_type, value,
                        );
                        let error = RowDataError::new(ordinal, &message);
                        work.flush()?;
                        Ok(CastRowResult::RowError(error))
                    }
                    None => Ok(CastRowResult::Null),
                };
            }
            macro_rules! convert {
                ($array:ty, $native:ty) => {{
                    let source = argument
                        .array()
                        .as_any()
                        .downcast_ref::<$array>()
                        .ok_or_else(|| internal("cast carrier has a foreign array implementation"))?
                        .value(row);
                    let output = match carrier_target {
                        Target::Boolean => Some(CastRowResult::Boolean(cast_num_to_bool(source))),
                        Target::Signed(SignedWidth::I8) => num_cast::<$native, i8>(source)
                            .map(|v| CastRowResult::Signed(i64::from(v))),
                        Target::Signed(SignedWidth::I16) => num_cast::<$native, i16>(source)
                            .map(|v| CastRowResult::Signed(i64::from(v))),
                        Target::Signed(SignedWidth::I32) => num_cast::<$native, i32>(source)
                            .map(|v| CastRowResult::Signed(i64::from(v))),
                        Target::Signed(SignedWidth::I64) => {
                            num_cast::<$native, i64>(source).map(CastRowResult::Signed)
                        }
                        Target::Unsigned(UnsignedWidth::U8) => num_cast::<$native, u8>(source)
                            .map(|v| CastRowResult::Unsigned(u64::from(v))),
                        Target::Unsigned(UnsignedWidth::U16) => num_cast::<$native, u16>(source)
                            .map(|v| CastRowResult::Unsigned(u64::from(v))),
                        Target::Unsigned(UnsignedWidth::U32) => num_cast::<$native, u32>(source)
                            .map(|v| CastRowResult::Unsigned(u64::from(v))),
                        Target::Unsigned(UnsignedWidth::U64) => {
                            num_cast::<$native, u64>(source).map(CastRowResult::Unsigned)
                        }
                        Target::F32 => num_cast::<$native, f32>(source).map(CastRowResult::Float32),
                        Target::F64 => num_cast::<$native, f64>(source).map(CastRowResult::Float64),
                        Target::Timestamp(_) => {
                            return Err(internal("numeric cast contains a foreign frozen target"));
                        }
                    };
                    work.step()?;
                    output.unwrap_or(CastRowResult::Null)
                }};
            }
            macro_rules! convert_float {
                ($array:ty, $native:ty, $f32:expr, $f64:expr) => {{
                    let source = argument.array().as_any().downcast_ref::<$array>()
                        .ok_or_else(|| internal("cast carrier has a foreign array implementation"))?
                        .value(row);
                    let converted = match carrier_target {
                        Target::Boolean => {
                            let value = cast_num_to_bool(source);
                            work.step()?;
                            return Ok(CastRowResult::Boolean(value));
                        }
                        Target::Signed(SignedWidth::I8) => num_cast::<$native, i8>(source).map(|v| CastRowResult::Signed(i64::from(v))),
                        Target::Signed(SignedWidth::I16) => num_cast::<$native, i16>(source).map(|v| CastRowResult::Signed(i64::from(v))),
                        Target::Signed(SignedWidth::I32) => num_cast::<$native, i32>(source).map(|v| CastRowResult::Signed(i64::from(v))),
                        Target::Signed(SignedWidth::I64) => num_cast::<$native, i64>(source).map(CastRowResult::Signed),
                        Target::Unsigned(UnsignedWidth::U8) => num_cast::<$native, u8>(source).map(|v| CastRowResult::Unsigned(u64::from(v))),
                        Target::Unsigned(UnsignedWidth::U16) => num_cast::<$native, u16>(source).map(|v| CastRowResult::Unsigned(u64::from(v))),
                        Target::Unsigned(UnsignedWidth::U32) => num_cast::<$native, u32>(source).map(|v| CastRowResult::Unsigned(u64::from(v))),
                        Target::Unsigned(UnsignedWidth::U64) => num_cast::<$native, u64>(source).map(CastRowResult::Unsigned),

                        Target::F32 => {
                            // Same-width identity must not quiet a signaling NaN
                            // by routing it through a different floating width.
                            let value = ($f32)(source);
                            work.step()?;
                            return Ok(CastRowResult::Float32(value));
                        }
                        Target::F64 => {
                            let value = ($f64)(source);
                            work.step()?;
                            return Ok(CastRowResult::Float64(value));
                        }
                        Target::Timestamp(_) => return Err(internal("floating cast contains a foreign frozen target")),
                    };
                    work.step()?;
                    match converted {
                        Some(value) => value,
                        None if !self.allow_throw_exception => CastRowResult::Null,
                        None => {
                            let name = match carrier_target {
                                Target::Signed(SignedWidth::I8) => "TINYINT",
                                Target::Signed(SignedWidth::I16) => "SMALLINT",
                                Target::Signed(SignedWidth::I32) => "INT",
                                Target::Signed(SignedWidth::I64) => "BIGINT",
                                Target::Unsigned(UnsignedWidth::U8) => "TINYINT UNSIGNED",
                                Target::Unsigned(UnsignedWidth::U16) => "SMALLINT UNSIGNED",
                                Target::Unsigned(UnsignedWidth::U32) => "INT UNSIGNED",
                                Target::Unsigned(UnsignedWidth::U64) => "BIGINT UNSIGNED",
                                _ => return Err(internal("floating cast contains a foreign frozen target")),
                            };
                            work.step()?;
                            work.flush()?;
                            // The frozen scalar types and one f64 bound the diagnostic.
                            // String allocation/formatting internals are opaque, not a
                            // formal host memory grant or an internal work proof.
                            let message = format!(
                                "Expr evaluate meet error: CAST failed: from {:?} to {:?}: {} conflict with range of {}",
                                self.source.data_type, self.result.data_type, source as f64, name,
                            );
                            let error = RowDataError::new(ordinal, &message);
                            work.flush()?;
                            CastRowResult::RowError(error)
                        }
                    }
                }};
            }
            Ok(match source_kind {
                Source::Date32 => return Err(internal("date cast escaped its checked operation")),
                Source::Utf8 => return Err(internal("text cast escaped its checked operation")),
                Source::Timestamp(_) => {
                    return Err(internal("timestamp cast escaped its checked operation"));
                }
                Source::Boolean => {
                    let value = argument
                        .array()
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| internal("cast carrier has a foreign array implementation"))?
                        .value(row);
                    let result = match carrier_target {
                        Target::Boolean => CastRowResult::Boolean(value),
                        Target::Signed(_) => CastRowResult::Signed(i64::from(value)),
                        Target::Unsigned(_) => CastRowResult::Unsigned(u64::from(value)),
                        Target::F32 => CastRowResult::Float32(f32::from(u8::from(value))),
                        Target::F64 => CastRowResult::Float64(f64::from(u8::from(value))),
                        Target::Timestamp(_) => {
                            return Err(internal("boolean cast contains a foreign frozen target"));
                        }
                    };
                    work.step()?;
                    result
                }
                Source::Signed(SignedWidth::I8) => convert!(Int8Array, i8),
                Source::Signed(SignedWidth::I16) => convert!(Int16Array, i16),
                Source::Signed(SignedWidth::I32) => convert!(Int32Array, i32),
                Source::Signed(SignedWidth::I64) => convert!(Int64Array, i64),
                Source::Unsigned(UnsignedWidth::U8) => convert!(UInt8Array, u8),
                Source::Unsigned(UnsignedWidth::U16) => convert!(UInt16Array, u16),
                Source::Unsigned(UnsignedWidth::U32) => convert!(UInt32Array, u32),
                Source::Unsigned(UnsignedWidth::U64) => convert!(UInt64Array, u64),
                Source::F32 => {
                    convert_float!(Float32Array, f32, |value: f32| value, |value: f32| value
                        as f64)
                }
                Source::F64 => convert_float!(
                    Float64Array,
                    f64,
                    |value: f64| value as f32,
                    |value: f64| value
                ),
            })
        })();
        // The same work latch returns any original callback cause without replay.
        work.finish()?;
        outcome
    }
    fn checked_row(
        &self,
        source_kind: Source,
        argument: EvaluatedArgument<'_>,
        ordinal: usize,
        logical_row: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<usize, KernelFailure> {
        self.checked_row_with_shape(argument, ordinal, logical_row, work, |array| {
            source_kind.validate(array)
        })
    }
    fn checked_row_with_shape(
        &self,
        argument: EvaluatedArgument<'_>,
        ordinal: usize,
        logical_row: usize,
        work: &mut EvaluationCheckpoints<'_>,
        concrete_shape: impl FnOnce(&dyn Array) -> bool,
    ) -> Result<usize, KernelFailure> {
        if let EvaluatedArgument::Constant(value) = argument {
            let actual = value.value_type();
            let matches = actual.logical_type == self.source.logical_type
                && (!actual.nullable || self.source.nullable);
            work.step()?;
            if !matches {
                return Err(invalid("cast constant differs from its frozen value type"));
            }
        }
        let array = argument.array();
        let matches = array.data_type() == &self.source.data_type;
        work.step()?;
        if !matches {
            return Err(invalid(
                "cast argument differs from its frozen numeric carrier",
            ));
        }
        let shape = match argument {
            EvaluatedArgument::Scalar(array) => array.len() == 1,
            EvaluatedArgument::SelectedColumn(values) => {
                values.selection().row(ordinal) == Some(logical_row)
            }
            _ => true,
        };
        work.step()?;
        if !shape {
            return Err(invalid(
                "cast argument has a foreign scalar or compact address",
            ));
        }
        if let EvaluatedArgument::SelectedColumn(values) = argument {
            let (mut start, mut end) = (0, values.errors().len());
            while start < end {
                let middle = start + (end - start) / 2;
                let actual = values.errors()[middle].selected_ordinal();
                work.step()?;
                match actual.cmp(&ordinal) {
                    std::cmp::Ordering::Less => start = middle + 1,
                    std::cmp::Ordering::Greater => end = middle,
                    std::cmp::Ordering::Equal => {
                        return Err(invalid(
                            "cast argument contains an unresolved selected row error",
                        ));
                    }
                }
            }
        }
        let row = argument.value_row(ordinal, logical_row);
        let in_bounds = row < array.len();
        work.step()?;
        if !in_bounds {
            return Err(invalid("cast selected address is outside its array"));
        }
        let concrete = concrete_shape(array.as_ref());
        work.step()?;
        if !concrete {
            return Err(internal("cast carrier has a foreign array implementation"));
        }
        Ok(row)
    }
}

#[cfg(test)]
#[path = "cast_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "cast_float_tests.rs"]
mod float_tests;

#[cfg(test)]
#[path = "cast_float_identity_tests.rs"]
mod float_identity_tests;

#[cfg(test)]
#[path = "cast_bool_tests.rs"]
mod bool_tests;

#[cfg(test)]
#[path = "cast_unsigned_tests.rs"]
mod unsigned_tests;

#[cfg(test)]
#[path = "cast_timestamp_tests.rs"]
mod timestamp_tests;

#[cfg(test)]
#[path = "cast_temporal_carrier_tests.rs"]
mod temporal_carrier_tests;

#[cfg(test)]
#[path = "cast_decimal_text_tests.rs"]
mod decimal_text_tests;

#[cfg(test)]
#[path = "cast_largeint_text_tests.rs"]
mod largeint_text_tests;

#[cfg(test)]
#[path = "cast_date_float_tests.rs"]
mod date_float_tests;

#[cfg(test)]
#[path = "cast_float_date_tests.rs"]
mod float_date_tests;

#[cfg(test)]
#[path = "cast_binary_text_tests.rs"]
mod binary_text_tests;

#[cfg(test)]
#[path = "cast_decimal_float_tests.rs"]
mod decimal_float_tests;

#[cfg(test)]
#[path = "cast_decimal_float32_tests.rs"]
mod decimal_float32_tests;

#[cfg(test)]
#[path = "cast_decimal128_rescale_tests.rs"]
mod decimal128_rescale_tests;

#[cfg(test)]
#[path = "cast_observed_list_tests.rs"]
mod observed_list_tests;

#[cfg(test)]
#[path = "cast_float_decimal128_tests.rs"]
mod float_decimal128_tests;
