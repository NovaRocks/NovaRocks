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

//! Selected scalar conversions retained from the original Arrow/decimal authors.
//! Every opaque Arrow invocation receives one already selected source row.
use super::*;
use arrow_array::ArrayRef;
use arrow_buffer::i256;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Body {
    DecimalInteger,
    Decimal256Integer,
    TextFloat,
    TextDecimal,
    DecimalNarrow,
    DateInteger,
    BooleanDecimal,
    Decimal256Boolean,
    CalendarNumeric { unit: Option<TimeUnit> },
}

pub(super) fn profile(source: &DataType, target: &DataType) -> Option<Body> {
    use DataType::*;
    let integer = SignedWidth::from_type(target).is_some();
    match (source, target) {
        (Decimal128(..), _) if integer => Some(Body::DecimalInteger),
        (Decimal256(..), _) if integer => Some(Body::Decimal256Integer),
        (Utf8, Float32 | Float64) => Some(Body::TextFloat),
        (Utf8, Decimal128(..)) => Some(Body::TextDecimal),
        (Decimal256(..), Decimal128(..)) => Some(Body::DecimalNarrow),
        (Date32, Int32 | Int64) => Some(Body::DateInteger),
        (Boolean, Decimal128(..)) => Some(Body::BooleanDecimal),
        (Decimal256(..), Boolean) => Some(Body::Decimal256Boolean),
        (Decimal128(..) | FixedSizeBinary(16), Date32) => {
            Some(Body::CalendarNumeric { unit: None })
        }
        (Decimal128(..) | FixedSizeBinary(16), Timestamp(unit, None)) => {
            Some(Body::CalendarNumeric { unit: Some(*unit) })
        }
        _ => None,
    }
}

impl Body {
    pub(super) fn can_produce_null(
        self,
        _source: &DataType,
        policy: DecimalOverflowPolicy,
        allow: bool,
    ) -> bool {
        match self {
            Self::TextFloat | Self::TextDecimal | Self::CalendarNumeric { .. } => true,
            Self::DecimalInteger | Self::Decimal256Integer | Self::BooleanDecimal => {
                policy == DecimalOverflowPolicy::OutputNull
            }
            Self::DecimalNarrow => policy == DecimalOverflowPolicy::OutputNull && !allow,
            Self::DateInteger | Self::Decimal256Boolean => false,
        }
    }

    pub(super) fn may_raise(
        self,
        source: &DataType,
        policy: DecimalOverflowPolicy,
        allow: bool,
    ) -> bool {
        match self {
            Self::CalendarNumeric { unit } => unit == Some(TimeUnit::Nanosecond),
            Self::DateInteger | Self::Decimal256Boolean | Self::TextFloat => false,
            Self::Decimal256Integer => policy == DecimalOverflowPolicy::ReportError,
            Self::DecimalInteger => {
                policy == DecimalOverflowPolicy::ReportError
                    || matches!(source, DataType::Decimal128(_, scale) if scale.unsigned_abs() > 38)
            }
            // Arrow's static decimal factor/metadata failure is a row error;
            // it remains observable even for a selected NULL source.
            Self::TextDecimal | Self::DecimalNarrow | Self::BooleanDecimal => {
                let _ = allow;
                true
            }
        }
    }

    pub(super) fn evaluate(
        self,
        recipe: &PreparedCastRecipe,
        argument: EvaluatedArgument<'_>,
        ordinal: usize,
        logical_row: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<CastRowResult, KernelFailure> {
        let row = recipe.checked_row_with_shape(argument, ordinal, logical_row, work, |array| {
            match recipe.source.data_type() {
                DataType::Decimal128(..) => array.as_any().is::<Decimal128Array>(),
                DataType::Decimal256(..) => array.as_any().is::<Decimal256Array>(),
                DataType::Utf8 => array.as_any().is::<StringArray>(),
                DataType::Date32 => array.as_any().is::<Date32Array>(),
                DataType::Boolean => array.as_any().is::<BooleanArray>(),
                DataType::FixedSizeBinary(16) => array.as_any().is::<FixedSizeBinaryArray>(),
                _ => false,
            }
        })?;
        let source_null = logical_is_null(argument.array().as_ref(), row, 1, work)?;
        if source_null && !recipe.source.nullable() {
            return Err(invalid(
                "non-null extended CAST source contains a selected NULL",
            ));
        }
        if let Self::CalendarNumeric { unit } = self {
            if source_null {
                return Ok(CastRowResult::Null);
            }
            work.flush()?;
            let selected = argument.array().slice(row, 1);
            let value = super::f3_array::numeric_i128(&selected)?
                .and_then(|value| i64::try_from(value).ok());
            work.flush()?;
            let Some(value) = value else {
                return Ok(CastRowResult::Null);
            };
            let carrier = Int64Array::from(vec![value]);
            work.flush()?;
            return calendar::evaluate(
                Source::Signed(SignedWidth::I64),
                &carrier,
                0,
                ordinal,
                unit,
                work,
            );
        }
        if self == Self::DateInteger {
            if source_null {
                return Ok(CastRowResult::Null);
            }
            let value = argument
                .array()
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| internal("DATE epoch projection has a foreign carrier"))?
                .value(row);
            work.step()?;
            return Ok(CastRowResult::Signed(i64::from(value)));
        }
        if self == Self::Decimal256Boolean {
            if source_null {
                return Ok(CastRowResult::Null);
            }
            let value = argument
                .array()
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| internal("decimal boolean CAST has a foreign carrier"))?
                .value(row);
            work.step()?;
            return Ok(CastRowResult::Boolean(value != i256::ZERO));
        }
        if self == Self::Decimal256Integer {
            if source_null {
                return Ok(CastRowResult::Null);
            }
            let DataType::Decimal256(_, scale) = recipe.source.data_type() else {
                return Err(internal("decimal integer CAST lost its frozen scale"));
            };
            let mut factor = Some(i256::ONE);
            for _ in 0..scale.unsigned_abs() {
                work.step()?;
                factor = factor.and_then(|value| value.checked_mul(i256::from_i128(10)));
            }
            let value = argument
                .array()
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| internal("decimal integer CAST has a foreign carrier"))?
                .value(row);
            let integral = factor
                .and_then(|factor| {
                    if *scale < 0 {
                        value.checked_mul(factor)
                    } else {
                        value.checked_div(factor)
                    }
                })
                .and_then(|value| value.to_i128())
                .and_then(|value| i64::try_from(value).ok())
                .and_then(
                    |value| match SignedWidth::from_type(recipe.result.data_type()) {
                        Some(SignedWidth::I8) => i8::try_from(value).ok().map(i64::from),
                        Some(SignedWidth::I16) => i16::try_from(value).ok().map(i64::from),
                        Some(SignedWidth::I32) => i32::try_from(value).ok().map(i64::from),
                        Some(SignedWidth::I64) => Some(value),
                        None => None,
                    },
                );
            work.step()?;
            return finish_numeric_null(recipe, ordinal, integral.map(CastRowResult::Signed), work);
        }
        if self == Self::DecimalNarrow {
            let (
                DataType::Decimal256(source_precision, source_scale),
                DataType::Decimal128(target_precision, target_scale),
            ) = (recipe.source.data_type(), recipe.result.data_type())
            else {
                return Err(internal("decimal narrowing CAST lost its frozen types"));
            };
            let coefficient = if source_null {
                None
            } else {
                Some(
                    argument
                        .array()
                        .as_any()
                        .downcast_ref::<Decimal256Array>()
                        .ok_or_else(|| internal("decimal narrowing CAST has a foreign carrier"))?
                        .value(row),
                )
            };
            if let Some(value) = super::f3_array::arrow_decimal_wide_delta(
                coefficient,
                *source_precision,
                *source_scale,
                *target_precision,
                *target_scale,
                arrow_schema::DECIMAL128_MAX_PRECISION,
            ) {
                work.step()?;
                if source_null {
                    return Ok(CastRowResult::Null);
                }
                let value = value
                    .and_then(|value| value.to_i128())
                    .map(CastRowResult::Decimal128);
                return finish_numeric_null(recipe, ordinal, value, work);
            }
        }
        if self == Self::BooleanDecimal {
            let DataType::Decimal128(precision, scale) = recipe.result.data_type() else {
                return Err(internal("boolean decimal CAST lost its frozen target"));
            };
            let mut factor = Some(1_i128);
            for _ in 0..scale.unsigned_abs() {
                work.step()?;
                factor = factor.and_then(|value| value.checked_mul(10));
            }
            let Some(factor) = factor else {
                return row_error(
                    ordinal,
                    "decimal scale overflow while casting BOOLEAN",
                    work,
                );
            };
            if source_null {
                return Ok(CastRowResult::Null);
            }
            let value = argument
                .array()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| internal("boolean decimal CAST has a foreign carrier"))?
                .value(row);
            let scaled = if *scale < 0 {
                Some(i128::from(value) / factor)
            } else {
                i128::from(value).checked_mul(factor)
            };
            let limit = crate::legacy_decimal::checked_pow10_i128(usize::from(*precision))
                .ok_or_else(|| internal("validated decimal precision lost its finite bound"))?;
            let scaled = scaled.filter(|value| *value < limit && *value > -limit);
            work.step()?;
            return finish_numeric_null(
                recipe,
                ordinal,
                scaled.map(CastRowResult::Decimal128),
                work,
            );
        }
        work.flush()?;
        let selected = argument.array().slice(row, 1);
        work.flush()?;
        let selected: ArrayRef = if self == Self::TextDecimal && !source_null {
            let text = selected
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| internal("decimal parser has a foreign text carrier"))?;
            // Empty text was normalized to NULL by the old shared scalar path.
            if text.value(0).trim().is_empty() {
                work.flush()?;
                let normalized = Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef;
                work.flush()?;
                normalized
            } else {
                selected
            }
        } else {
            selected
        };
        work.flush()?;
        let converted = if matches!(self, Self::TextFloat | Self::TextDecimal) {
            let text = selected
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| internal("raw numeric parser has a foreign text carrier"))?;
            crate::math_numeric::cast_raw_numeric(text, recipe.result.data_type())
                .map_err(super::f3_array::ArrayCastFailure::RawNumeric)
        } else {
            arrow_cast::cast(selected.as_ref(), recipe.result.data_type())
                .map_err(super::f3_array::ArrayCastFailure::Arrow)
        };
        let output = match converted {
            Ok(output) => output,
            Err(error) => {
                let row = error.into_row(ordinal)?;
                work.flush()?;
                return Ok(CastRowResult::RowError(row));
            }
        };
        work.flush()?;
        let value = if output.is_null(0) {
            None
        } else {
            macro_rules! value {
                ($array:ty, $map:expr) => {{
                    let value = output
                        .as_any()
                        .downcast_ref::<$array>()
                        .ok_or_else(|| internal("extended CAST returned a foreign frozen carrier"))?
                        .value(0);
                    Some(($map)(value))
                }};
            }
            match recipe.result.data_type() {
                DataType::Int8 => value!(Int8Array, |v| CastRowResult::Signed(i64::from(v))),
                DataType::Int16 => value!(Int16Array, |v| CastRowResult::Signed(i64::from(v))),
                DataType::Int32 => value!(Int32Array, |v| CastRowResult::Signed(i64::from(v))),
                DataType::Int64 => value!(Int64Array, CastRowResult::Signed),
                DataType::Decimal128(..) => value!(Decimal128Array, CastRowResult::Decimal128),
                DataType::Float32 => {
                    let value = output
                        .as_any()
                        .downcast_ref::<Float32Array>()
                        .ok_or_else(|| internal("text float CAST returned a foreign carrier"))?
                        .value(0);
                    value.is_finite().then_some(CastRowResult::Float32(value))
                }
                DataType::Float64 => {
                    let value = output
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .ok_or_else(|| internal("text float CAST returned a foreign carrier"))?
                        .value(0);
                    value.is_finite().then_some(CastRowResult::Float64(value))
                }
                _ => return Err(internal("extended scalar CAST lost its prepared target")),
            }
        };
        if matches!(self, Self::TextDecimal | Self::TextFloat) || source_null {
            return Ok(value.unwrap_or(CastRowResult::Null));
        }
        finish_numeric_null(recipe, ordinal, value, work)
    }
}

fn finish_numeric_null(
    recipe: &PreparedCastRecipe,
    ordinal: usize,
    value: Option<CastRowResult>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<CastRowResult, KernelFailure> {
    if let Some(value) = value {
        return Ok(value);
    }
    let decimal_to_decimal = matches!(
        recipe.source.data_type(),
        DataType::Decimal128(..) | DataType::Decimal256(..)
    ) && matches!(
        recipe.result.data_type(),
        DataType::Decimal128(..) | DataType::Decimal256(..)
    );
    if recipe.decimal_overflow_policy == DecimalOverflowPolicy::ReportError
        || (recipe.allow_throw_exception && decimal_to_decimal)
    {
        return row_error(
            ordinal,
            "Expr evaluate meet error: The numeric type cast involving decimal overflows",
            work,
        );
    }
    Ok(CastRowResult::Null)
}

fn row_error(
    ordinal: usize,
    message: &str,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<CastRowResult, KernelFailure> {
    work.flush()?;
    let error = RowDataError::with_reason(
        ordinal,
        crate::DataFailureReason::NumericOutOfRange,
        message,
    );
    work.flush()?;
    Ok(CastRowResult::RowError(error))
}

#[cfg(test)]
#[path = "f3_scalar/tests.rs"]
mod tests;
