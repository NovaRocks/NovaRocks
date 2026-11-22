// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::{
    Failure,
    resources::{Context, invalid},
    wire,
};
use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use novarocks_proto_models::{plan, value_type as logical};
use novarocks_type_contract::{MAX_ARROW_TIMESTAMP_TIMEZONE_BYTES, ValueKind};
use wire::arrow_physical_type::Kind;

fn primitive(value: plan::ArrowPrimitiveType) -> Kind {
    Kind::Primitive(value as i32)
}
fn encode_time_unit(unit: TimeUnit) -> plan::ArrowTimeUnit {
    match unit {
        TimeUnit::Second => plan::ArrowTimeUnit::Second,
        TimeUnit::Millisecond => plan::ArrowTimeUnit::Millisecond,
        TimeUnit::Microsecond => plan::ArrowTimeUnit::Microsecond,
        TimeUnit::Nanosecond => plan::ArrowTimeUnit::Nanosecond,
    }
}
fn encode_interval_unit(unit: IntervalUnit) -> plan::ArrowIntervalUnit {
    match unit {
        IntervalUnit::YearMonth => plan::ArrowIntervalUnit::YearMonth,
        IntervalUnit::DayTime => plan::ArrowIntervalUnit::DayTime,
        IntervalUnit::MonthDayNano => plan::ArrowIntervalUnit::MonthDayNano,
    }
}
fn decimal(precision: u8, scale: i8) -> plan::ArrowDecimalType {
    plan::ArrowDecimalType {
        precision: u32::from(precision),
        scale: i32::from(scale),
    }
}
pub(super) fn encode<H>(
    data_type: &DataType,
    context: &mut Context<'_, '_, H>,
) -> Result<Kind, Failure<H>> {
    Ok(match data_type {
        DataType::Null => primitive(plan::ArrowPrimitiveType::Null),
        DataType::Boolean => primitive(plan::ArrowPrimitiveType::Boolean),
        DataType::Int8 => primitive(plan::ArrowPrimitiveType::Int8),
        DataType::Int16 => primitive(plan::ArrowPrimitiveType::Int16),
        DataType::Int32 => primitive(plan::ArrowPrimitiveType::Int32),
        DataType::Int64 => primitive(plan::ArrowPrimitiveType::Int64),
        DataType::UInt8 => primitive(plan::ArrowPrimitiveType::Uint8),
        DataType::UInt16 => primitive(plan::ArrowPrimitiveType::Uint16),
        DataType::UInt32 => primitive(plan::ArrowPrimitiveType::Uint32),
        DataType::UInt64 => primitive(plan::ArrowPrimitiveType::Uint64),
        DataType::Float16 => primitive(plan::ArrowPrimitiveType::Float16),
        DataType::Float32 => primitive(plan::ArrowPrimitiveType::Float32),
        DataType::Float64 => primitive(plan::ArrowPrimitiveType::Float64),
        DataType::Date32 => primitive(plan::ArrowPrimitiveType::Date32),
        DataType::Date64 => primitive(plan::ArrowPrimitiveType::Date64),
        DataType::Binary => primitive(plan::ArrowPrimitiveType::Binary),
        DataType::BinaryView => primitive(plan::ArrowPrimitiveType::BinaryView),
        DataType::LargeBinary => primitive(plan::ArrowPrimitiveType::LargeBinary),
        DataType::Utf8 => primitive(plan::ArrowPrimitiveType::Utf8),
        DataType::Utf8View => primitive(plan::ArrowPrimitiveType::Utf8View),
        DataType::LargeUtf8 => primitive(plan::ArrowPrimitiveType::LargeUtf8),
        DataType::Timestamp(unit, timezone) => Kind::Timestamp(plan::ArrowTimestampType {
            unit: encode_time_unit(*unit) as i32,
            timezone: timezone
                .as_ref()
                .map(|value| context.string(value))
                .transpose()?,
        }),
        DataType::Time32(unit) => Kind::Time32(plan::ArrowTimeType {
            unit: encode_time_unit(*unit) as i32,
        }),
        DataType::Time64(unit) => Kind::Time64(plan::ArrowTimeType {
            unit: encode_time_unit(*unit) as i32,
        }),
        DataType::Duration(unit) => Kind::Duration(plan::ArrowTimeType {
            unit: encode_time_unit(*unit) as i32,
        }),
        DataType::Interval(unit) => Kind::Interval(encode_interval_unit(*unit) as i32),
        DataType::FixedSizeBinary(width) => Kind::FixedSizeBinary(*width),
        DataType::Decimal32(precision, scale) => Kind::Decimal32(decimal(*precision, *scale)),
        DataType::Decimal64(precision, scale) => Kind::Decimal64(decimal(*precision, *scale)),
        DataType::Decimal128(precision, scale) => Kind::Decimal128(decimal(*precision, *scale)),
        DataType::Decimal256(precision, scale) => Kind::Decimal256(decimal(*precision, *scale)),
        _ => return Err(invalid("encoding is not a scalar value layout")),
    })
}

fn time<H>(unit: i32) -> Result<TimeUnit, Failure<H>> {
    Ok(match plan::ArrowTimeUnit::try_from(unit).ok() {
        Some(plan::ArrowTimeUnit::Second) => TimeUnit::Second,
        Some(plan::ArrowTimeUnit::Millisecond) => TimeUnit::Millisecond,
        Some(plan::ArrowTimeUnit::Microsecond) => TimeUnit::Microsecond,
        Some(plan::ArrowTimeUnit::Nanosecond) => TimeUnit::Nanosecond,
        _ => return Err(invalid("invalid value layout time unit")),
    })
}
fn interval<H>(unit: i32) -> Result<IntervalUnit, Failure<H>> {
    Ok(match plan::ArrowIntervalUnit::try_from(unit).ok() {
        Some(plan::ArrowIntervalUnit::YearMonth) => IntervalUnit::YearMonth,
        Some(plan::ArrowIntervalUnit::DayTime) => IntervalUnit::DayTime,
        Some(plan::ArrowIntervalUnit::MonthDayNano) => IntervalUnit::MonthDayNano,
        _ => return Err(invalid("invalid value layout interval unit")),
    })
}
fn decode_primitive<H>(value: i32) -> Result<DataType, Failure<H>> {
    use plan::ArrowPrimitiveType as P;
    Ok(match P::try_from(value).ok() {
        Some(P::Null) => DataType::Null,
        Some(P::Boolean) => DataType::Boolean,
        Some(P::Int8) => DataType::Int8,
        Some(P::Int16) => DataType::Int16,
        Some(P::Int32) => DataType::Int32,
        Some(P::Int64) => DataType::Int64,
        Some(P::Uint8) => DataType::UInt8,
        Some(P::Uint16) => DataType::UInt16,
        Some(P::Uint32) => DataType::UInt32,
        Some(P::Uint64) => DataType::UInt64,
        Some(P::Float16) => DataType::Float16,
        Some(P::Float32) => DataType::Float32,
        Some(P::Float64) => DataType::Float64,
        Some(P::Date32) => DataType::Date32,
        Some(P::Date64) => DataType::Date64,
        Some(P::Binary) => DataType::Binary,
        Some(P::BinaryView) => DataType::BinaryView,
        Some(P::LargeBinary) => DataType::LargeBinary,
        Some(P::Utf8) => DataType::Utf8,
        Some(P::Utf8View) => DataType::Utf8View,
        Some(P::LargeUtf8) => DataType::LargeUtf8,
        _ => return Err(invalid("invalid value layout primitive")),
    })
}
fn decode_decimal<H>(
    value: &plan::ArrowDecimalType,
    kind: logical::ScalarKind,
    make: fn(u8, i8) -> DataType,
) -> Result<DataType, Failure<H>> {
    let scalar = logical::ScalarValue {
        kind: kind as i32,
        precision: Some(value.precision),
        scale: Some(value.scale),
        ..Default::default()
    };
    crate::value_type_table::scalars::validate::<H>(&scalar)?;
    Ok(make(value.precision as u8, value.scale as i8))
}
/// Parameter validation borrows all strings and constructs no owned projection.
/// Timestamp timezone storage is intentionally absent in this temporary scalar.
pub(super) fn validate<H>(
    kind: &Kind,
    context: &mut Context<'_, '_, H>,
) -> Result<DataType, Failure<H>> {
    context.step()?;
    Ok(match kind {
        Kind::Primitive(value) => decode_primitive(*value)?,
        Kind::Timestamp(value) => {
            if let Some(zone) = &value.timezone {
                if zone.len() > MAX_ARROW_TIMESTAMP_TIMEZONE_BYTES {
                    return Err(invalid("value layout timezone exceeds byte bound"));
                }
                for _ in zone.bytes() {
                    context.step()?;
                }
            }
            DataType::Timestamp(time(value.unit)?, None)
        }
        Kind::Time32(value) => {
            let unit = time(value.unit)?;
            if !matches!(unit, TimeUnit::Second | TimeUnit::Millisecond) {
                return Err(invalid("invalid value layout Time32 unit"));
            }
            DataType::Time32(unit)
        }
        Kind::Time64(value) => {
            let unit = time(value.unit)?;
            if !matches!(unit, TimeUnit::Microsecond | TimeUnit::Nanosecond) {
                return Err(invalid("invalid value layout Time64 unit"));
            }
            DataType::Time64(unit)
        }
        Kind::Duration(value) => DataType::Duration(time(value.unit)?),
        Kind::Interval(value) => DataType::Interval(interval(*value)?),
        Kind::FixedSizeBinary(width) => {
            if *width < 0 {
                return Err(invalid("negative value layout fixed binary width"));
            }
            DataType::FixedSizeBinary(*width)
        }
        Kind::Decimal32(value) => {
            decode_decimal(value, logical::ScalarKind::Decimal32, DataType::Decimal32)?
        }
        Kind::Decimal64(value) => {
            decode_decimal(value, logical::ScalarKind::Decimal64, DataType::Decimal64)?
        }
        Kind::Decimal128(value) => {
            decode_decimal(value, logical::ScalarKind::Decimal128, DataType::Decimal128)?
        }
        Kind::Decimal256(value) => {
            decode_decimal(value, logical::ScalarKind::Decimal256, DataType::Decimal256)?
        }
        _ => return Err(invalid("non-scalar value layout leaf")),
    })
}
pub(super) fn decode<H>(
    kind: &Kind,
    context: &mut Context<'_, '_, H>,
) -> Result<DataType, Failure<H>> {
    decode_with_source_facts(kind, context).map(|(value, _)| value)
}
pub(super) fn decode_with_source_facts<H>(
    kind: &Kind,
    context: &mut Context<'_, '_, H>,
) -> Result<(DataType, usize), Failure<H>> {
    let data_type = validate(kind, context)?;
    if let Kind::Timestamp(value) = kind {
        let unit = time(value.unit)?;
        let (timezone, bytes) = match value.timezone.as_deref() {
            Some(zone) => {
                let (zone, bytes) = context.timezone_with_source_facts(zone)?;
                (Some(zone), bytes)
            }
            None => (None, 0),
        };
        return Ok((DataType::Timestamp(unit, timezone), bytes));
    }
    Ok((data_type, 0))
}
pub(super) fn compatible<H>(
    expected: &ValueKind,
    kind: &Kind,
    actual: &DataType,
    context: &mut Context<'_, '_, H>,
) -> Result<bool, Failure<H>> {
    Ok(match (expected, actual) {
        (
            ValueKind::Scalar(DataType::Timestamp(expected_unit, expected_zone)),
            DataType::Timestamp(actual_unit, _),
        ) => {
            let Kind::Timestamp(wire) = kind else {
                return Ok(false);
            };
            if expected_unit != actual_unit {
                false
            } else {
                same_optional(expected_zone.as_deref(), wire.timezone.as_deref(), context)?
            }
        }
        (ValueKind::Scalar(expected), actual) => expected == actual,
        (
            ValueKind::String | ValueKind::BoundedCharacter(_),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
        ) => true,
        (ValueKind::Binary, DataType::Binary | DataType::LargeBinary | DataType::BinaryView) => {
            true
        }
        (ValueKind::Nominal(nominal), actual) => nominal.validate_carrier(actual).is_ok(),
        _ => false,
    })
}
fn same_optional<H>(
    a: Option<&str>,
    b: Option<&str>,
    context: &mut Context<'_, '_, H>,
) -> Result<bool, Failure<H>> {
    match (a, b) {
        (None, None) => Ok(true),
        (Some(a), Some(b)) => {
            if a.len() != b.len() {
                return Ok(false);
            };
            for (left, right) in a.bytes().zip(b.bytes()) {
                context.step()?;
                if left != right {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        _ => Ok(false),
    }
}
