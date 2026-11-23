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

use std::fmt;

use novarocks_failure_contract::{FailureClass, FailureFact, FailureProjection, FailureReason};
use novarocks_function_contract::kernel_failure::{
    DATE_RESULT_OUT_OF_RANGE, INVALID_CAST_ARGUMENT, INVALID_FUNCTION_ARGUMENT,
    NUMERIC_OUT_OF_RANGE,
};
use novarocks_type_contract::{
    PICOS_PER_DAY, TemporalMicros, TemporalPrecision, TemporalType, TemporalValue,
};

use crate::datetime_value::date32_to_gregorian;
use crate::temporal_arithmetic::Value;
use crate::temporal_value::{gregorian_epoch_day, round_time, round_timestamp};
use crate::temporal_zone_rules::{TemporalZoneSource, joda_local_to_utc, utc_to_local};

const MILLIS_PER_DAY: i64 = 86_400_000;
const PICOS_PER_MILLI: i128 = 1_000_000_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Unit {
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

impl Unit {
    pub(super) fn parse(text: &str) -> Option<Self> {
        const UNITS: [(&str, Unit); 9] = [
            ("millisecond", Unit::Millisecond),
            ("second", Unit::Second),
            ("minute", Unit::Minute),
            ("hour", Unit::Hour),
            ("day", Unit::Day),
            ("week", Unit::Week),
            ("month", Unit::Month),
            ("quarter", Unit::Quarter),
            ("year", Unit::Year),
        ];
        UNITS
            .iter()
            .find(|(name, _)| text.eq_ignore_ascii_case(name))
            .map(|(_, unit)| *unit)
    }

    fn is_calendar(self) -> bool {
        matches!(
            self,
            Self::Day | Self::Week | Self::Month | Self::Quarter | Self::Year
        )
    }

    fn millis(self) -> Option<i64> {
        Some(match self {
            Self::Millisecond => 1,
            Self::Second => 1_000,
            Self::Minute => 60_000,
            Self::Hour => 3_600_000,
            Self::Day => MILLIS_PER_DAY,
            Self::Week => MILLIS_PER_DAY * 7,
            Self::Month | Self::Quarter | Self::Year => return None,
        })
    }
}

#[derive(Debug)]
pub(super) enum ShiftError {
    InvalidUnit,
    InvalidPattern,
    Arithmetic,
    TimestampRange,
    DateRange(i64),
    Cast,
    ForeignValue,
}

impl fmt::Display for ShiftError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidUnit => f.write_str("Invalid calendar unit for the selected type"),
            Self::InvalidPattern => f.write_str("Invalid date pattern or input"),
            Self::Arithmetic => f.write_str("Date or time arithmetic result is out of range"),
            Self::TimestampRange => f.write_str("Timestamp out of range"),
            Self::DateRange(days) => write!(f, "Day ordinal {days} exceeds the range for DATE"),
            Self::Cast => f.write_str("Cannot cast value to DATE"),
            Self::ForeignValue => f.write_str("Calendar call received a foreign value"),
        }
    }
}

impl std::error::Error for ShiftError {}

impl FailureProjection for ShiftError {
    fn failure_fact(&self) -> FailureFact {
        let reason: FailureReason = match self {
            Self::InvalidUnit | Self::InvalidPattern | Self::Arithmetic => {
                INVALID_FUNCTION_ARGUMENT
            }
            Self::TimestampRange => NUMERIC_OUT_OF_RANGE,
            Self::DateRange(_) => DATE_RESULT_OUT_OF_RANGE,
            Self::Cast => INVALID_CAST_ARGUMENT,
            Self::ForeignValue => {
                return crate::kernel_control::invalid("Calendar call received a foreign value")
                    .failure_fact();
            }
        };
        FailureFact::new(FailureClass::Data, reason)
    }
}

pub(super) fn checked_date(days: i64) -> Result<i32, ShiftError> {
    i32::try_from(days).map_err(|_| ShiftError::DateRange(days))
}

pub(super) fn add(
    unit: Unit,
    amount: i64,
    ty: Option<TemporalType>,
    value: Value,
) -> Result<Value, ShiftError> {
    use TemporalType as T;
    use TemporalValue as V;
    Ok(match (ty, value) {
        (None, Value::Date(day)) => {
            if !unit.is_calendar() {
                return Err(ShiftError::InvalidUnit);
            }
            let millis = shift_millis(i64::from(day) * MILLIS_PER_DAY, unit, amount)
                .ok_or(ShiftError::Arithmetic)?;
            Value::Date(checked_date(millis.div_euclid(MILLIS_PER_DAY))?)
        }
        (Some(T::Time(p)), Value::Temporal(V::Time(value))) => {
            Value::Temporal(V::Time(shift_time(value, unit, amount, p)?))
        }
        (
            Some(T::TimeWithTimeZone(p)),
            Value::Temporal(V::TimeWithTimeZone {
                picos,
                offset_minutes,
            }),
        ) => Value::Temporal(V::TimeWithTimeZone {
            picos: shift_time(picos, unit, amount, p)?,
            offset_minutes,
        }),
        (Some(T::Timestamp(p)), Value::Temporal(V::Timestamp(value))) => {
            let shifted = shift_stamp(value, unit, amount, p).ok_or(ShiftError::TimestampRange)?;
            Value::Temporal(V::Timestamp(shifted))
        }
        (
            Some(T::TimestampWithTimeZone(p)),
            Value::Temporal(V::TimestampWithTimeZone { instant, zone }),
        ) => {
            let shift = || {
                if unit.is_calendar() {
                    let local = utc_to_local(zone, TemporalZoneSource::Joda, instant).ok()?;
                    let shifted = shift_stamp(local, unit, amount, TemporalPrecision::PICOSECONDS)?;
                    let shifted = joda_local_to_utc(zone, shifted, Some(instant)).ok()?;
                    round_millisecond_shift(shifted, p)
                } else {
                    shift_stamp(instant, unit, amount, p)
                }
            };
            let instant = shift().ok_or(ShiftError::Arithmetic)?;
            let result = V::TimestampWithTimeZone { instant, zone };
            result
                .validate(T::TimestampWithTimeZone(p))
                .map_err(|_| ShiftError::Arithmetic)?;
            Value::Temporal(result)
        }
        _ => return Err(ShiftError::ForeignValue),
    })
}

fn shift_time(
    value: i64,
    unit: Unit,
    amount: i64,
    precision: TemporalPrecision,
) -> Result<i64, ShiftError> {
    if unit.is_calendar() {
        return Err(ShiftError::InvalidUnit);
    }
    let unit = unit.millis().ok_or(ShiftError::InvalidUnit)?;
    let delta = i128::from(amount % (MILLIS_PER_DAY / unit)) * i128::from(unit) * PICOS_PER_MILLI;
    let picos = (i128::from(value) + delta).rem_euclid(i128::from(PICOS_PER_DAY)) as i64;
    round_time(picos, precision).map_err(|_| ShiftError::Arithmetic)
}

fn shift_stamp(
    value: TemporalMicros,
    unit: Unit,
    amount: i64,
    precision: TemporalPrecision,
) -> Option<TemporalMicros> {
    let millis = shift_millis(value.floor_millis(), unit, amount)?;
    let fraction = value.as_picos().rem_euclid(PICOS_PER_MILLI);
    let at = TemporalMicros::from_picos(i128::from(millis) * PICOS_PER_MILLI + fraction)?;
    round_millisecond_shift(at, precision)
}

fn round_millisecond_shift(
    at: TemporalMicros,
    precision: TemporalPrecision,
) -> Option<TemporalMicros> {
    if precision <= TemporalPrecision::DEFAULT {
        round_timestamp(at, precision).ok()
    } else {
        Some(at)
    }
}

fn shift_millis(millis: i64, unit: Unit, amount: i64) -> Option<i64> {
    if let Some(duration) = unit.millis() {
        return millis.checked_add(duration.checked_mul(amount)?);
    }
    let month_stride = match unit {
        Unit::Month => 1,
        Unit::Quarter => 3,
        Unit::Year => 12,
        _ => return None,
    };
    let day = i32::try_from(millis.div_euclid(MILLIS_PER_DAY)).ok()?;
    let (year, month, day_of_month) = date32_to_gregorian(day);
    let month = (i64::from(year) * 12 + i64::from(month) - 1)
        .checked_add(amount.checked_mul(month_stride)?)?;
    let first = month_start(month)?;
    let next = month_start(month.checked_add(1)?)?;
    let clipped_day = i64::from(day_of_month).min(next - first);
    let shifted_day = first.checked_add(clipped_day - 1)?;
    shifted_day
        .checked_mul(MILLIS_PER_DAY)?
        .checked_add(millis.rem_euclid(MILLIS_PER_DAY))
}

fn month_start(month: i64) -> Option<i64> {
    gregorian_epoch_day(month.div_euclid(12), month.rem_euclid(12) + 1, 1).ok()
}

pub(super) fn truncate(
    unit: Unit,
    ty: Option<TemporalType>,
    value: Value,
) -> Result<Value, ShiftError> {
    use TemporalType as T;
    use TemporalValue as V;
    Ok(match (ty, value) {
        (None, Value::Date(days)) if unit.is_calendar() => {
            let millis = truncate_millis(i64::from(days) * MILLIS_PER_DAY, unit)
                .ok_or(ShiftError::Arithmetic)?;
            Value::Date(checked_date(millis.div_euclid(MILLIS_PER_DAY))?)
        }
        (None, Value::Date(_)) => return Err(ShiftError::InvalidUnit),
        (Some(T::Time(_)), Value::Temporal(V::Time(picos))) => {
            Value::Temporal(V::Time(truncate_time(picos, unit)?))
        }
        (
            Some(T::TimeWithTimeZone(_)),
            Value::Temporal(V::TimeWithTimeZone {
                picos,
                offset_minutes,
            }),
        ) => Value::Temporal(V::TimeWithTimeZone {
            picos: truncate_time(picos, unit)?,
            offset_minutes,
        }),
        (Some(T::Timestamp(_)), Value::Temporal(V::Timestamp(stamp))) => {
            let millis =
                truncate_millis(stamp.floor_millis(), unit).ok_or(ShiftError::TimestampRange)?;
            let instant = stamp_from_millis(millis, ShiftError::TimestampRange)?;
            Value::Temporal(V::Timestamp(instant))
        }
        (
            Some(T::TimestampWithTimeZone(p)),
            Value::Temporal(V::TimestampWithTimeZone { instant, zone }),
        ) => {
            let local = utc_to_local(zone, TemporalZoneSource::Joda, instant)
                .map_err(|_| ShiftError::Arithmetic)?;
            let local_floor =
                truncate_millis(local.floor_millis(), unit).ok_or(ShiftError::Arithmetic)?;
            let instant = if unit.is_calendar() {
                let local = stamp_from_millis(local_floor, ShiftError::Arithmetic)?;
                joda_local_to_utc(zone, local, Some(instant)).map_err(|_| ShiftError::Arithmetic)?
            } else {
                let offset = local.floor_millis() - instant.floor_millis();
                let millis = local_floor
                    .checked_sub(offset)
                    .ok_or(ShiftError::Arithmetic)?;
                stamp_from_millis(millis, ShiftError::Arithmetic)?
            };
            let result = V::TimestampWithTimeZone { instant, zone };
            result
                .validate(T::TimestampWithTimeZone(p))
                .map_err(|_| ShiftError::Arithmetic)?;
            Value::Temporal(result)
        }
        _ => return Err(ShiftError::ForeignValue),
    })
}

fn truncate_time(picos: i64, unit: Unit) -> Result<i64, ShiftError> {
    if unit.is_calendar() {
        return Err(ShiftError::InvalidUnit);
    }
    let step = unit.millis().ok_or(ShiftError::InvalidUnit)? * PICOS_PER_MILLI as i64;
    Ok(picos - picos.rem_euclid(step))
}

fn truncate_millis(millis: i64, unit: Unit) -> Option<i64> {
    let step = match unit {
        Unit::Week | Unit::Month | Unit::Quarter | Unit::Year => {
            let day = millis.div_euclid(MILLIS_PER_DAY);
            if unit == Unit::Week {
                // Epoch Thursday is three days after the start of its ISO week.
                return day
                    .checked_sub((day + 3).rem_euclid(7))?
                    .checked_mul(MILLIS_PER_DAY);
            }
            let (year, month, _) = date32_to_gregorian(i32::try_from(day).ok()?);
            let start = match unit {
                Unit::Month => month,
                Unit::Quarter => (month - 1) / 3 * 3 + 1,
                _ => 1,
            };
            return gregorian_epoch_day(i64::from(year), i64::from(start), 1)
                .ok()?
                .checked_mul(MILLIS_PER_DAY);
        }
        _ => unit.millis()?,
    };
    millis.checked_sub(millis.rem_euclid(step))
}

pub(super) fn difference(unit: Unit, first: Value, second: Value) -> Result<i64, ShiftError> {
    use TemporalValue as V;
    match (first, second) {
        (Value::Date(first), Value::Date(second)) if unit.is_calendar() => calendar_difference(
            unit,
            i64::from(first) * MILLIS_PER_DAY,
            i64::from(second) * MILLIS_PER_DAY,
        ),
        (Value::Date(_), Value::Date(_)) => Err(ShiftError::InvalidUnit),
        (Value::Temporal(V::Time(first)), Value::Temporal(V::Time(second))) => {
            time_difference(unit, first, second)
        }
        (
            Value::Temporal(V::TimeWithTimeZone {
                picos: first,
                offset_minutes: first_offset,
            }),
            Value::Temporal(V::TimeWithTimeZone {
                picos: second,
                offset_minutes: second_offset,
            }),
        ) => {
            use novarocks_type_contract::PICOS_PER_MINUTE;
            let normalize = |picos: i64, offset: i16| {
                (picos - i64::from(offset) * PICOS_PER_MINUTE).rem_euclid(PICOS_PER_DAY)
            };
            time_difference(
                unit,
                normalize(first, first_offset),
                normalize(second, second_offset),
            )
        }
        (Value::Temporal(V::Timestamp(first)), Value::Temporal(V::Timestamp(second))) => {
            calendar_difference(unit, first.floor_millis(), second.floor_millis())
        }
        (
            Value::Temporal(V::TimestampWithTimeZone {
                instant: first,
                zone,
            }),
            Value::Temporal(V::TimestampWithTimeZone {
                instant: second, ..
            }),
        ) => {
            let local_millis = |at: TemporalMicros| {
                utc_to_local(zone, TemporalZoneSource::Joda, at)
                    .map(TemporalMicros::floor_millis)
                    .map_err(|_| ShiftError::Arithmetic)
            };
            if unit.is_calendar() {
                calendar_difference(unit, local_millis(first)?, local_millis(second)?)
            } else {
                calendar_difference(unit, first.floor_millis(), second.floor_millis())
            }
        }
        _ => Err(ShiftError::ForeignValue),
    }
}

fn time_difference(unit: Unit, first: i64, second: i64) -> Result<i64, ShiftError> {
    if unit.is_calendar() {
        return Err(ShiftError::InvalidUnit);
    }
    let step = unit.millis().ok_or(ShiftError::InvalidUnit)? * PICOS_PER_MILLI as i64;
    Ok((second - first) / step)
}

fn calendar_difference(unit: Unit, first: i64, second: i64) -> Result<i64, ShiftError> {
    if let Some(step) = unit.millis() {
        return second
            .checked_sub(first)
            .map(|delta| delta / step)
            .ok_or(ShiftError::Arithmetic);
    }
    if first > second {
        return calendar_difference(unit, second, first).map(|difference| -difference);
    }
    let parts = |millis: i64| {
        i32::try_from(millis.div_euclid(MILLIS_PER_DAY))
            .map(date32_to_gregorian)
            .map_err(|_| ShiftError::Arithmetic)
    };
    let (year1, month1, _) = parts(first)?;
    let (year2, month2, _) = parts(second)?;
    let years = i64::from(year2) - i64::from(year1);
    let months = years * 12 + i64::from(month2) - i64::from(month1);
    let (field, candidate, divisor) = match unit {
        Unit::Month => (Unit::Month, months, 1),
        Unit::Quarter => (Unit::Month, months, 3),
        Unit::Year => (Unit::Year, years, 1),
        _ => return Err(ShiftError::InvalidUnit),
    };
    let shifted = shift_millis(first, field, candidate).ok_or(ShiftError::Arithmetic)?;
    Ok((candidate - i64::from(shifted > second)) / divisor)
}

pub(super) fn last_day(value: Value) -> Result<i32, ShiftError> {
    use TemporalValue as V;
    let millis = match value {
        Value::Date(days) => return month_end(i64::from(days)),
        Value::Temporal(V::Timestamp(stamp)) => stamp.floor_millis(),
        Value::Temporal(V::TimestampWithTimeZone { instant, zone }) => {
            let at = instant
                .floor_millis()
                .checked_add(1)
                .ok_or(ShiftError::Arithmetic)?;
            let at = stamp_from_millis(at, ShiftError::Arithmetic)?;
            let local = utc_to_local(zone, TemporalZoneSource::Joda, at)
                .map_err(|_| ShiftError::Arithmetic)?;
            let millis = local.floor_millis();
            let floor = truncate_millis(millis, Unit::Month).ok_or(ShiftError::Arithmetic)?;
            let ceiling = if millis == floor {
                floor
            } else {
                shift_millis(floor, Unit::Month, 1).ok_or(ShiftError::Arithmetic)?
            };
            let local = stamp_from_millis(ceiling, ShiftError::Arithmetic)?;
            let utc =
                joda_local_to_utc(zone, local, Some(at)).map_err(|_| ShiftError::Arithmetic)?;
            let next = utc_to_local(zone, TemporalZoneSource::Joda, utc)
                .map_err(|_| ShiftError::Arithmetic)?;
            let previous = next
                .floor_millis()
                .checked_sub(MILLIS_PER_DAY)
                .ok_or(ShiftError::Arithmetic)?;
            return checked_date(previous / MILLIS_PER_DAY);
        }
        _ => return Err(ShiftError::ForeignValue),
    };
    month_end(millis.div_euclid(MILLIS_PER_DAY))
}

fn month_end(day: i64) -> Result<i32, ShiftError> {
    let (year, month, _) = date32_to_gregorian(checked_date(day)?);
    let next =
        month_start(i64::from(year) * 12 + i64::from(month)).ok_or(ShiftError::Arithmetic)?;
    checked_date(next - 1)
}

fn stamp_from_millis(millis: i64, reason: ShiftError) -> Result<TemporalMicros, ShiftError> {
    TemporalMicros::from_picos(i128::from(millis) * PICOS_PER_MILLI).ok_or(reason)
}
