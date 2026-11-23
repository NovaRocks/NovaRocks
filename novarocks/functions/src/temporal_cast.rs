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

use arrow_schema::{DataType, TimeUnit};
use novarocks_type_contract::{
    FunctionValueType, IntervalField, PICOS_PER_DAY, PICOS_PER_MINUTE, PICOS_PER_SECOND,
    SemanticParameterKey, StatementStartUtc, TemporalMicros, TemporalPrecision, TemporalType,
    TemporalValue, TimeZoneKey, ValueLogicalType,
};

use crate::temporal_value::{
    LiteralError, ParsedTemporal, cast_precision, date_to_timestamp, interval_leading_magnitude,
    parse_interval, parse_time_literal, parse_timestamp_cast, parse_zoned_time_cast, round_time,
    round_timestamp,
};
use crate::temporal_zone_rules::{
    TemporalZoneSource, joda_local_to_utc, offset_seconds, utc_to_local,
};

const SECONDS_PER_DAY: i64 = 86_400;
const MICROS_PER_DAY: i64 = SECONDS_PER_DAY * 1_000_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Type {
    Temporal(TemporalType),
    Date,
    Text,
    Integer,
    Bigint,
    LegacyTimestamp,
}

impl Type {
    fn project(ty: &FunctionValueType) -> Option<Self> {
        if let Some(ty) = ty.value_type().temporal() {
            return Some(Self::Temporal(ty));
        }
        if ty.logical_type() != ValueLogicalType::Physical {
            return None;
        }
        Some(match ty.data_type() {
            DataType::Date32 => Self::Date,
            DataType::Utf8 => Self::Text,
            DataType::Int32 => Self::Integer,
            DataType::Int64 => Self::Bigint,
            DataType::Timestamp(TimeUnit::Microsecond, None) => Self::LegacyTimestamp,
            _ => return None,
        })
    }

    fn normalized(self) -> Self {
        match self {
            Self::LegacyTimestamp => {
                Self::Temporal(TemporalType::Timestamp(TemporalPrecision::MICROSECONDS))
            }
            ty => ty,
        }
    }

    pub(crate) fn temporal(self) -> Option<TemporalType> {
        match self.normalized() {
            Self::Temporal(ty) => Some(ty),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Dependencies {
    None,
    Zone,
    Clock,
}

impl Dependencies {
    pub(crate) fn keys(self) -> &'static [SemanticParameterKey] {
        use SemanticParameterKey::{StatementStartUtc, TimeZone};
        match self {
            Self::None => &[],
            Self::Zone => &[TimeZone],
            Self::Clock => &[StatementStartUtc, TimeZone],
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Pair {
    pub(crate) source: Type,
    pub(crate) target: Type,
    pub(crate) dependencies: Dependencies,
}

impl Pair {
    pub(crate) fn bind(source: &FunctionValueType, target: &FunctionValueType) -> Option<Self> {
        use Dependencies as D;
        use TemporalType::*;
        use Type::*;
        let source = Type::project(source)?;
        let target = Type::project(target)?;
        if !matches!(source, Temporal(_)) && !matches!(target, Temporal(_)) {
            return None;
        }
        let dependencies = match (source.normalized(), target.normalized()) {
            (
                Temporal(Time(_)),
                Temporal(TimeWithTimeZone(_) | Timestamp(_) | TimestampWithTimeZone(_)),
            )
            | (Temporal(TimeWithTimeZone(_)), Temporal(Timestamp(_) | TimestampWithTimeZone(_)))
            | (Temporal(Timestamp(_)), Temporal(TimeWithTimeZone(_)))
            | (Text, Temporal(TimeWithTimeZone(_))) => D::Clock,
            (Date | Text | Temporal(Timestamp(_)), Temporal(TimestampWithTimeZone(_))) => D::Zone,
            (Text, Temporal(_))
            | (Temporal(_), Text)
            | (Date, Temporal(Timestamp(_)))
            | (Temporal(Timestamp(_) | TimestampWithTimeZone(_)), Date)
            | (Temporal(Time(_)), Temporal(Time(_)))
            | (Temporal(TimeWithTimeZone(_)), Temporal(Time(_) | TimeWithTimeZone(_)))
            | (Temporal(Timestamp(_)), Temporal(Time(_) | Timestamp(_)))
            | (
                Temporal(TimestampWithTimeZone(_)),
                Temporal(Time(_) | TimeWithTimeZone(_) | Timestamp(_) | TimestampWithTimeZone(_)),
            ) => D::None,
            (Temporal(Interval(left)), Temporal(Interval(right)))
                if left.is_year_month() == right.is_year_month() =>
            {
                D::None
            }
            (Integer | Bigint, Temporal(Interval(qualifier)))
            | (Temporal(Interval(qualifier)), Bigint)
                if qualifier.start() == qualifier.end() =>
            {
                D::None
            }
            _ => return None,
        };
        Some(Self {
            source,
            target,
            dependencies,
        })
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Facts {
    pub(crate) zone: Option<TimeZoneKey>,
    pub(crate) start: Option<StatementStartUtc>,
}

impl Facts {
    fn clock(self) -> Result<(i64, i32), LiteralError> {
        let zone = self.zone.ok_or(LiteralError::InvalidZone)?;
        let start = self.start.ok_or(LiteralError::Range)?;
        let millis = start
            .seconds()
            .checked_mul(1000)
            .and_then(|seconds| seconds.checked_add(i64::from(start.nanos() / 1_000_000)))
            .ok_or(LiteralError::Range)?;
        let offset = offset_seconds(zone, TemporalZoneSource::Jdk, millis)
            .map_err(|_| LiteralError::Range)?;
        let local_seconds = start
            .seconds()
            .checked_add(i64::from(offset))
            .ok_or(LiteralError::Range)?;
        Ok((local_seconds.div_euclid(SECONDS_PER_DAY), offset))
    }
}

#[derive(Debug)]
pub(crate) enum Input<'a> {
    Date(i32),
    Integral(i64),
    Text(&'a str),
    Temporal(TemporalValue),
}

#[derive(Debug)]
pub(crate) enum Output {
    Date(i32),
    Bigint(i64),
    Temporal(TemporalValue),
}

impl Pair {
    pub(crate) fn convert(self, input: Input<'_>, facts: Facts) -> Result<Output, LiteralError> {
        let target = self.target.normalized();
        if target == Type::Date {
            let local = match input {
                Input::Temporal(TemporalValue::Timestamp(value)) => value,
                Input::Temporal(TemporalValue::TimestampWithTimeZone { instant, zone }) => {
                    utc_to_local(zone, TemporalZoneSource::Joda, instant)
                        .map_err(|_| LiteralError::Range)?
                }
                _ => return Err(LiteralError::DifferentTemporalFamilies),
            };
            let day = i32::try_from(local.micros().div_euclid(MICROS_PER_DAY))
                .map_err(|_| LiteralError::Range)?;
            return Ok(Output::Date(day));
        }
        if target == Type::Bigint {
            let (Input::Temporal(value), Some(TemporalType::Interval(qualifier))) =
                (input, self.source.temporal())
            else {
                return Err(LiteralError::DifferentTemporalFamilies);
            };
            let unit = interval_unit(qualifier.start());
            let signed = match value {
                TemporalValue::IntervalYearMonth(months) => i128::from(months),
                TemporalValue::IntervalDayTime(value) => value.as_picos(),
                _ => return Err(LiteralError::DifferentTemporalFamilies),
            };
            return Ok(Output::Bigint(
                i64::try_from(signed / unit).map_err(|_| LiteralError::IntervalRange)?,
            ));
        }
        let Type::Temporal(target) = target else {
            return Err(LiteralError::DifferentTemporalFamilies);
        };
        let value = match input {
            Input::Date(days) => cast_date(days, target, facts)?,
            Input::Integral(value) => {
                let TemporalType::Interval(qualifier) = target else {
                    return Err(LiteralError::DifferentTemporalFamilies);
                };
                let units = i128::from(value) * interval_unit(qualifier.start());
                let result = if qualifier.is_year_month() {
                    TemporalValue::IntervalYearMonth(
                        i32::try_from(units).map_err(|_| LiteralError::IntervalRange)?,
                    )
                } else {
                    TemporalValue::IntervalDayTime(
                        TemporalMicros::from_picos(units).ok_or(LiteralError::IntervalRange)?,
                    )
                };
                check_leading(result, qualifier)?;
                result
            }
            Input::Text(text) => cast_text(text, target, facts)?,
            Input::Temporal(value) => {
                let source = self
                    .source
                    .temporal()
                    .ok_or(LiteralError::DifferentTemporalFamilies)?;
                cast_temporal(ParsedTemporal { ty: source, value }, target, facts)?
            }
        };
        Ok(Output::Temporal(
            value.validate(target).map_err(|_| LiteralError::Range)?,
        ))
    }
}

fn cast_date(days: i32, target: TemporalType, facts: Facts) -> Result<TemporalValue, LiteralError> {
    let local = date_to_timestamp(days, target.precision())?;
    let TemporalValue::Timestamp(value) = local.value else {
        unreachable!("the date conversion produces a local timestamp");
    };
    match target {
        TemporalType::Timestamp(_) => Ok(local.value),
        TemporalType::TimestampWithTimeZone(_) => {
            let zone = facts.zone.ok_or(LiteralError::InvalidZone)?;
            let offset = offset_seconds(zone, TemporalZoneSource::Joda, value.floor_millis())
                .map_err(|_| LiteralError::Range)?;
            Ok(TemporalValue::TimestampWithTimeZone {
                instant: shifted(value, -offset)?,
                zone,
            })
        }
        _ => Err(LiteralError::DifferentTemporalFamilies),
    }
}

fn cast_text(
    text: &str,
    target: TemporalType,
    facts: Facts,
) -> Result<TemporalValue, LiteralError> {
    use TemporalType::*;
    Ok(match target {
        Timestamp(_) | TimestampWithTimeZone(_) => {
            parse_timestamp_cast(
                text.trim_matches(crate::datetime_value::trino_whitespace),
                target,
                facts.zone,
            )?
            .value
        }
        Time(precision) => {
            let TemporalValue::Time(picos) = parse_time_literal(text)?.value else {
                return Err(LiteralError::Syntax);
            };
            TemporalValue::Time(round_time(picos, precision)?)
        }
        TimeWithTimeZone(precision) => {
            let (picos, parsed) = parse_zoned_time_cast(
                text.trim_matches(crate::datetime_value::trino_whitespace),
                precision,
            )?;
            let offset_minutes = match parsed {
                Some(offset) => offset,
                None => minutes(facts.clock()?.1)?,
            };
            TemporalValue::TimeWithTimeZone {
                picos,
                offset_minutes,
            }
        }
        Interval(qualifier) => {
            let value = parse_interval(text, qualifier, false)?;
            check_leading(value, qualifier)?;
            value
        }
    })
}

fn cast_temporal(
    source: ParsedTemporal,
    target: TemporalType,
    facts: Facts,
) -> Result<TemporalValue, LiteralError> {
    use TemporalType as T;
    use TemporalValue as V;
    let precision = target.precision();
    let time =
        |value: TemporalMicros| value.as_picos().rem_euclid(i128::from(PICOS_PER_DAY)) as i64;
    Ok(match (source.value, target) {
        (V::Time(_), T::Time(_))
        | (V::TimeWithTimeZone { .. }, T::TimeWithTimeZone(_))
        | (V::Timestamp(_), T::Timestamp(_))
        | (V::TimestampWithTimeZone { .. }, T::TimestampWithTimeZone(_)) => {
            cast_precision(source, target)?.value
        }
        (V::TimeWithTimeZone { picos, .. }, T::Time(_)) => V::Time(round_time(picos, precision)?),
        (V::Time(_) | V::Timestamp(_), T::TimeWithTimeZone(_)) => {
            let picos = match source.value {
                V::Time(picos) => picos,
                V::Timestamp(value) => time(value),
                _ => unreachable!("selected local time source"),
            };
            V::TimeWithTimeZone {
                picos: round_time(picos, precision)?,
                offset_minutes: minutes(facts.clock()?.1)?,
            }
        }
        (V::Timestamp(value), T::Time(_)) => V::Time(round_time(time(value), precision)?),
        (V::Timestamp(value), T::TimestampWithTimeZone(_)) => {
            let zone = facts.zone.ok_or(LiteralError::InvalidZone)?;
            let local = round_timestamp(value, precision)?;
            let instant = joda_local_to_utc(zone, local, None).map_err(|_| LiteralError::Range)?;
            V::TimestampWithTimeZone { instant, zone }
        }
        (V::TimestampWithTimeZone { instant, zone }, T::Timestamp(_)) => {
            let local = utc_to_local(zone, TemporalZoneSource::Joda, instant)
                .map_err(|_| LiteralError::Range)?;
            V::Timestamp(round_timestamp(local, precision)?)
        }
        (V::TimestampWithTimeZone { instant, zone }, T::Time(_)) => {
            let local = utc_to_local(zone, TemporalZoneSource::Jdk, instant)
                .map_err(|_| LiteralError::Range)?;
            V::Time(round_time(time(local), precision)?)
        }
        (V::TimestampWithTimeZone { instant, zone }, T::TimeWithTimeZone(_)) => {
            let local = utc_to_local(zone, TemporalZoneSource::Joda, instant)
                .map_err(|_| LiteralError::Range)?;
            let offset = offset_seconds(zone, TemporalZoneSource::Jdk, local.floor_millis())
                .map_err(|_| LiteralError::Range)?;
            V::TimeWithTimeZone {
                picos: round_time(time(local), precision)?,
                offset_minutes: minutes(offset)?,
            }
        }
        (V::Time(picos) | V::TimeWithTimeZone { picos, .. }, T::Timestamp(_)) => {
            let (days, _) = facts.clock()?;
            V::Timestamp(round_timestamp(timestamp_of_day(days, picos)?, precision)?)
        }
        (V::Time(picos), T::TimestampWithTimeZone(_)) => {
            let (days, offset) = facts.clock()?;
            let zone = facts.zone.ok_or(LiteralError::InvalidZone)?;
            let local = round_timestamp(timestamp_of_day(days, picos)?, precision)?;
            V::TimestampWithTimeZone {
                instant: shifted(local, -offset)?,
                zone,
            }
        }
        (
            V::TimeWithTimeZone {
                picos,
                offset_minutes,
            },
            T::TimestampWithTimeZone(_),
        ) => {
            let (days, _) = facts.clock()?;
            let zone = TimeZoneKey::from_offset_minutes(offset_minutes)
                .ok_or(LiteralError::InvalidZone)?;
            let utc_picos = (picos - i64::from(offset_minutes) * 60 * PICOS_PER_SECOND)
                .rem_euclid(PICOS_PER_DAY);
            let instant = round_timestamp(timestamp_of_day(days, utc_picos)?, precision)?;
            V::TimestampWithTimeZone { instant, zone }
        }
        (V::IntervalYearMonth(months), T::Interval(qualifier)) => {
            let months = match qualifier.end() {
                IntervalField::Year => months / 12 * 12,
                IntervalField::Month => months,
                _ => return Err(LiteralError::DifferentTemporalFamilies),
            };
            let value = V::IntervalYearMonth(months);
            check_leading(value, qualifier)?;
            value
        }
        (V::IntervalDayTime(value), T::Interval(qualifier)) => {
            let value = match qualifier.end() {
                IntervalField::Second => {
                    round_timestamp(value, precision).map_err(|_| LiteralError::IntervalRange)?
                }
                field @ (IntervalField::Day | IntervalField::Hour | IntervalField::Minute) => {
                    let unit = interval_unit(field);
                    TemporalMicros::from_picos(value.as_picos() / unit * unit)
                        .ok_or(LiteralError::IntervalRange)?
                }
                _ => return Err(LiteralError::DifferentTemporalFamilies),
            };
            let value = V::IntervalDayTime(value);
            check_leading(value, qualifier)?;
            value
        }
        _ => return Err(LiteralError::DifferentTemporalFamilies),
    })
}

// Units are months for a year-month qualifier and picoseconds for a day-time qualifier.
fn interval_unit(field: IntervalField) -> i128 {
    i128::from(match field {
        IntervalField::Year => 12,
        IntervalField::Month => 1,
        IntervalField::Day => PICOS_PER_DAY,
        IntervalField::Hour => 60 * PICOS_PER_MINUTE,
        IntervalField::Minute => PICOS_PER_MINUTE,
        IntervalField::Second => PICOS_PER_SECOND,
    })
}

fn check_leading(
    value: TemporalValue,
    qualifier: novarocks_type_contract::IntervalQualifier,
) -> Result<(), LiteralError> {
    let magnitude = interval_leading_magnitude(value, qualifier.start())
        .ok_or(LiteralError::DifferentTemporalFamilies)?;
    let max = 10u64.pow(u32::from(qualifier.leading_precision()));
    if magnitude >= max {
        return Err(LiteralError::IntervalRange);
    }
    Ok(())
}

fn shifted(value: TemporalMicros, seconds: i32) -> Result<TemporalMicros, LiteralError> {
    TemporalMicros::from_picos(
        value.as_picos() + i128::from(seconds) * i128::from(PICOS_PER_SECOND),
    )
    .ok_or(LiteralError::Range)
}

fn timestamp_of_day(days: i64, picos: i64) -> Result<TemporalMicros, LiteralError> {
    TemporalMicros::from_picos(i128::from(days) * i128::from(PICOS_PER_DAY) + i128::from(picos))
        .ok_or(LiteralError::Range)
}

fn minutes(seconds: i32) -> Result<i16, LiteralError> {
    i16::try_from(seconds / 60).map_err(|_| LiteralError::InvalidZone)
}
