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
    DATE_RESULT_OUT_OF_RANGE, INVALID_FUNCTION_ARGUMENT, NUMERIC_OUT_OF_RANGE,
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
    Arithmetic,
    TimestampRange,
    DateRange(i64),
    ForeignValue,
}

impl fmt::Display for ShiftError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidUnit => f.write_str("Invalid date_add unit for the selected type"),
            Self::Arithmetic => f.write_str("Date or time arithmetic result is out of range"),
            Self::TimestampRange => f.write_str("Timestamp out of range"),
            Self::DateRange(days) => write!(f, "Day ordinal {days} exceeds the range for DATE"),
            Self::ForeignValue => f.write_str("date_add received a foreign value"),
        }
    }
}

impl std::error::Error for ShiftError {}

impl FailureProjection for ShiftError {
    fn failure_fact(&self) -> FailureFact {
        let reason: FailureReason = match self {
            Self::InvalidUnit | Self::Arithmetic => INVALID_FUNCTION_ARGUMENT,
            Self::TimestampRange => NUMERIC_OUT_OF_RANGE,
            Self::DateRange(_) => DATE_RESULT_OUT_OF_RANGE,
            Self::ForeignValue => {
                return crate::kernel_control::invalid("date_add received a foreign value")
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
