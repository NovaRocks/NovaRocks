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

use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use novarocks_type_contract::{CharacterKind, CharacterType, ValueKind, ValueLogicalType};

use super::{
    Failure,
    resources::{Context, invalid},
    wire,
};
use wire::ScalarKind as S;

pub(crate) fn time<H>(unit: i32) -> Result<TimeUnit, Failure<H>> {
    Ok(match wire::TimeUnit::try_from(unit).ok() {
        Some(wire::TimeUnit::Second) => TimeUnit::Second,
        Some(wire::TimeUnit::Millisecond) => TimeUnit::Millisecond,
        Some(wire::TimeUnit::Microsecond) => TimeUnit::Microsecond,
        Some(wire::TimeUnit::Nanosecond) => TimeUnit::Nanosecond,
        _ => return Err(invalid("invalid logical scalar time unit")),
    })
}
fn encode_time(unit: &TimeUnit) -> i32 {
    (match unit {
        TimeUnit::Second => wire::TimeUnit::Second,
        TimeUnit::Millisecond => wire::TimeUnit::Millisecond,
        TimeUnit::Microsecond => wire::TimeUnit::Microsecond,
        TimeUnit::Nanosecond => wire::TimeUnit::Nanosecond,
    }) as i32
}
fn interval<H>(unit: i32) -> Result<IntervalUnit, Failure<H>> {
    Ok(match wire::IntervalUnit::try_from(unit).ok() {
        Some(wire::IntervalUnit::YearMonth) => IntervalUnit::YearMonth,
        Some(wire::IntervalUnit::DayTime) => IntervalUnit::DayTime,
        Some(wire::IntervalUnit::MonthDayNano) => IntervalUnit::MonthDayNano,
        _ => return Err(invalid("invalid logical scalar interval unit")),
    })
}
pub(crate) fn nominal<H>(value: i32) -> Result<ValueLogicalType, Failure<H>> {
    Ok(match wire::NominalValue::try_from(value).ok() {
        Some(wire::NominalValue::Json) => ValueLogicalType::Json,
        Some(wire::NominalValue::Variant) => ValueLogicalType::Variant,
        Some(wire::NominalValue::Hll) => ValueLogicalType::Hll,
        Some(wire::NominalValue::Bitmap) => ValueLogicalType::Bitmap,
        Some(wire::NominalValue::Object) => ValueLogicalType::Object,
        Some(wire::NominalValue::Percentile) => ValueLogicalType::Percentile,
        Some(wire::NominalValue::LargeInt) => ValueLogicalType::LargeInt,
        Some(wire::NominalValue::Uuid) => ValueLogicalType::Uuid,
        _ => return Err(invalid("invalid logical nominal domain")),
    })
}
pub(crate) fn encode_nominal<H>(value: ValueLogicalType) -> Result<i32, Failure<H>> {
    Ok(match value {
        ValueLogicalType::Json => wire::NominalValue::Json,
        ValueLogicalType::Variant => wire::NominalValue::Variant,
        ValueLogicalType::Hll => wire::NominalValue::Hll,
        ValueLogicalType::Bitmap => wire::NominalValue::Bitmap,
        ValueLogicalType::Object => wire::NominalValue::Object,
        ValueLogicalType::Percentile => wire::NominalValue::Percentile,
        ValueLogicalType::LargeInt => wire::NominalValue::LargeInt,
        ValueLogicalType::Uuid => wire::NominalValue::Uuid,
        ValueLogicalType::Physical => return Err(invalid("Physical is not a logical domain")),
    } as i32)
}

/// Validate required and irrelevant fields without constructing any scalar.
pub(crate) fn validate<H>(value: &wire::ScalarValue) -> Result<S, Failure<H>> {
    let kind = S::try_from(value.kind).map_err(|_| invalid("unknown logical scalar kind"))?;
    let present = u8::from(value.fixed_binary_width.is_some())
        | (u8::from(value.precision.is_some()) << 1)
        | (u8::from(value.scale.is_some()) << 2)
        | (u8::from(value.time_unit.is_some()) << 3)
        | (u8::from(value.timezone.is_some()) << 4)
        | (u8::from(value.interval_unit.is_some()) << 5)
        | (u8::from(value.character_length.is_some()) << 6);
    let (required, allowed) = match kind {
        S::Unspecified => return Err(invalid("missing logical scalar kind")),
        S::FixedSizeBinary => (1, 1),
        S::Decimal32 | S::Decimal64 | S::Decimal128 | S::Decimal256 => (6, 6),
        S::Timestamp => (8, 24),
        S::Time32 | S::Time64 | S::Duration => (8, 8),
        S::Interval => (32, 32),
        S::Char | S::Varchar => (64, 64),
        _ => (0, 0),
    };
    if present & required != required || present & !allowed != 0 {
        return Err(invalid("missing or irrelevant logical scalar parameter"));
    }
    if let Some(length) = value.character_length {
        let character_kind = if kind == S::Char {
            CharacterKind::Char
        } else {
            CharacterKind::Varchar
        };
        if CharacterType::new(character_kind, length).is_none() {
            return Err(invalid("invalid logical character length"));
        }
    }
    if value.fixed_binary_width.is_some_and(|width| width < 0) {
        return Err(invalid("negative logical fixed binary width"));
    }
    if let Some(unit) = value.time_unit {
        let unit = time(unit)?;
        if (kind == S::Time32 && !matches!(unit, TimeUnit::Second | TimeUnit::Millisecond))
            || (kind == S::Time64 && !matches!(unit, TimeUnit::Microsecond | TimeUnit::Nanosecond))
        {
            return Err(invalid("invalid logical time width and unit"));
        }
    }
    if let Some(unit) = value.interval_unit {
        interval::<H>(unit)?;
    }
    if let (Some(precision), Some(scale)) = (value.precision, value.scale) {
        let maximum = match kind {
            S::Decimal32 => 9,
            S::Decimal64 => 18,
            S::Decimal128 => 38,
            S::Decimal256 => 76,
            _ => {
                return Err(invalid(
                    "decimal parameters on a non-decimal logical scalar",
                ));
            }
        };
        // This is Arrow's parameter grammar, including every negative i8 scale.
        if precision == 0
            || precision > maximum
            || scale < i32::from(i8::MIN)
            || scale > maximum as i32
            || (scale > 0 && scale as u32 > precision)
        {
            return Err(invalid("invalid logical decimal parameters"));
        }
    }
    Ok(kind)
}

pub(crate) fn decode_with_source_facts<H>(
    value: &wire::ScalarValue,
    context: &mut Context<'_, '_, H>,
) -> Result<(ValueKind, usize), Failure<H>> {
    let kind = validate(value)?;
    context.step()?;
    let mut retained = 0usize;
    let data_type = match kind {
        S::String => return Ok((ValueKind::String, 0)),
        S::Char | S::Varchar => {
            let character_kind = if kind == S::Char {
                CharacterKind::Char
            } else {
                CharacterKind::Varchar
            };
            let character = value
                .character_length
                .and_then(|length| CharacterType::new(character_kind, length))
                .ok_or_else(|| invalid("invalid logical character length"))?;
            return Ok((ValueKind::BoundedCharacter(character), 0));
        }
        S::Binary => return Ok((ValueKind::Binary, 0)),
        S::Null => DataType::Null,
        S::Boolean => DataType::Boolean,
        S::Int8 => DataType::Int8,
        S::Int16 => DataType::Int16,
        S::Int32 => DataType::Int32,
        S::Int64 => DataType::Int64,
        S::Uint8 => DataType::UInt8,
        S::Uint16 => DataType::UInt16,
        S::Uint32 => DataType::UInt32,
        S::Uint64 => DataType::UInt64,
        S::Float16 => DataType::Float16,
        S::Float32 => DataType::Float32,
        S::Float64 => DataType::Float64,
        S::FixedSizeBinary => DataType::FixedSizeBinary(value.fixed_binary_width.unwrap()),
        S::Date32 => DataType::Date32,
        S::Date64 => DataType::Date64,
        S::Timestamp => DataType::Timestamp(
            time(value.time_unit.unwrap())?,
            value
                .timezone
                .as_deref()
                .map(|zone| {
                    let (value, bytes) = context.timezone_with_source_facts(zone)?;
                    retained = bytes;
                    Ok::<_, Failure<H>>(value)
                })
                .transpose()?,
        ),
        S::Time32 => DataType::Time32(time(value.time_unit.unwrap())?),
        S::Time64 => DataType::Time64(time(value.time_unit.unwrap())?),
        S::Duration => DataType::Duration(time(value.time_unit.unwrap())?),
        S::Interval => DataType::Interval(interval(value.interval_unit.unwrap())?),
        S::Decimal32 => {
            DataType::Decimal32(value.precision.unwrap() as u8, value.scale.unwrap() as i8)
        }
        S::Decimal64 => {
            DataType::Decimal64(value.precision.unwrap() as u8, value.scale.unwrap() as i8)
        }
        S::Decimal128 => {
            DataType::Decimal128(value.precision.unwrap() as u8, value.scale.unwrap() as i8)
        }
        S::Decimal256 => {
            DataType::Decimal256(value.precision.unwrap() as u8, value.scale.unwrap() as i8)
        }
        S::Unspecified => return Err(invalid("missing logical scalar kind")),
    };
    Ok((ValueKind::Scalar(data_type), retained))
}

pub(crate) fn encode<H>(
    kind: &ValueKind,
    context: &mut Context<'_, '_, H>,
) -> Result<wire::ScalarValue, Failure<H>> {
    let mut value = wire::ScalarValue::default();
    let scalar = match kind {
        ValueKind::String => S::String,
        ValueKind::BoundedCharacter(character) => {
            value.character_length = Some(character.length());
            match character.kind() {
                CharacterKind::Char => S::Char,
                CharacterKind::Varchar => S::Varchar,
            }
        }
        ValueKind::Binary => S::Binary,
        ValueKind::Scalar(data_type) => match data_type {
            DataType::Null => S::Null,
            DataType::Boolean => S::Boolean,
            DataType::Int8 => S::Int8,
            DataType::Int16 => S::Int16,
            DataType::Int32 => S::Int32,
            DataType::Int64 => S::Int64,
            DataType::UInt8 => S::Uint8,
            DataType::UInt16 => S::Uint16,
            DataType::UInt32 => S::Uint32,
            DataType::UInt64 => S::Uint64,
            DataType::Float16 => S::Float16,
            DataType::Float32 => S::Float32,
            DataType::Float64 => S::Float64,
            DataType::FixedSizeBinary(width) => {
                value.fixed_binary_width = Some(*width);
                S::FixedSizeBinary
            }
            DataType::Date32 => S::Date32,
            DataType::Date64 => S::Date64,
            DataType::Timestamp(unit, zone) => {
                value.time_unit = Some(encode_time(unit));
                value.timezone = zone
                    .as_deref()
                    .map(|zone| context.string(zone))
                    .transpose()?;
                S::Timestamp
            }
            DataType::Time32(unit) => {
                value.time_unit = Some(encode_time(unit));
                S::Time32
            }
            DataType::Time64(unit) => {
                value.time_unit = Some(encode_time(unit));
                S::Time64
            }
            DataType::Duration(unit) => {
                value.time_unit = Some(encode_time(unit));
                S::Duration
            }
            DataType::Interval(unit) => {
                value.interval_unit = Some(match unit {
                    IntervalUnit::YearMonth => wire::IntervalUnit::YearMonth,
                    IntervalUnit::DayTime => wire::IntervalUnit::DayTime,
                    IntervalUnit::MonthDayNano => wire::IntervalUnit::MonthDayNano,
                } as i32);
                S::Interval
            }
            DataType::Decimal32(p, s)
            | DataType::Decimal64(p, s)
            | DataType::Decimal128(p, s)
            | DataType::Decimal256(p, s) => {
                value.precision = Some(u32::from(*p));
                value.scale = Some(i32::from(*s));
                match data_type {
                    DataType::Decimal32(..) => S::Decimal32,
                    DataType::Decimal64(..) => S::Decimal64,
                    DataType::Decimal128(..) => S::Decimal128,
                    _ => S::Decimal256,
                }
            }
            _ => return Err(invalid("container or encoding used as a logical scalar")),
        },
        _ => return Err(invalid("non-scalar logical kind used as a scalar")),
    };
    value.kind = scalar as i32;
    validate::<H>(&value)?;
    context.step()?;
    Ok(value)
}
