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

use novarocks_type_contract::{
    PICOS_PER_DAY, PICOS_PER_MINUTE, PICOS_PER_SECOND, TemporalValue, TimeZoneKey,
};

use super::date_shift::ShiftError;
use crate::{
    datetime_value::{date32_to_gregorian, gregorian_epoch_day_from_valid_parts},
    temporal_arithmetic::Value,
    temporal_zone_rules::{TemporalZoneSource, offset_seconds, utc_to_local},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Field {
    Year,
    Quarter,
    Month,
    Day,
    DayOfWeek,
    DayOfYear,
    Week,
    YearOfWeek,
    Hour,
    Minute,
    Second,
    TimeZoneHour,
    TimeZoneMinute,
}

impl Field {
    pub(super) fn evaluate(self, value: Value) -> Result<i64, ShiftError> {
        let parts = Parts::from_value(value)?;
        match self {
            Self::Hour => return Ok(parts.seconds_of_day / 3600),
            Self::Minute => return Ok(parts.seconds_of_day / 60 % 60),
            Self::Second => return Ok(parts.seconds_of_day % 60),
            Self::TimeZoneHour => return Ok(parts.offset_seconds / 3600),
            Self::TimeZoneMinute => return Ok(parts.offset_seconds / 60 % 60),
            _ => {}
        }
        let (year, month, day) = date32_to_gregorian(parts.days);
        let days = i64::from(parts.days);
        let year = i64::from(year);
        Ok(match self {
            Self::Year => year,
            Self::Quarter => i64::from((month - 1) / 3 + 1),
            Self::Month => i64::from(month),
            Self::Day => i64::from(day),
            Self::DayOfWeek => (days + 3).rem_euclid(7) + 1,
            Self::DayOfYear => days - gregorian_epoch_day_from_valid_parts(year, 1, 1) + 1,
            Self::Week => {
                let (_, start) = iso_week_year(days, year);
                (days - start) / 7 + 1
            }
            Self::YearOfWeek => iso_week_year(days, year).0,
            _ => return Err(ShiftError::ForeignValue),
        })
    }
}

pub(super) fn iso_week_year(days: i64, gregorian_year: i64) -> (i64, i64) {
    let week_one = |year| {
        let jan4 = gregorian_epoch_day_from_valid_parts(year, 1, 4);
        jan4 - (jan4 + 3).rem_euclid(7)
    };
    let start = week_one(gregorian_year);
    if days < start {
        (gregorian_year - 1, week_one(gregorian_year - 1))
    } else {
        let next = week_one(gregorian_year + 1);
        if days >= next {
            (gregorian_year + 1, next)
        } else {
            (gregorian_year, start)
        }
    }
}

struct Parts {
    days: i32,
    seconds_of_day: i64,
    offset_seconds: i64,
}

impl Parts {
    fn from_value(value: Value) -> Result<Self, ShiftError> {
        use TemporalValue as T;
        match value {
            Value::Date(days) => Ok(Self {
                days,
                seconds_of_day: 0,
                offset_seconds: 0,
            }),
            Value::Temporal(T::Time(picos)) => Self::local(i128::from(picos), 0),
            Value::Temporal(T::TimeWithTimeZone {
                picos,
                offset_minutes,
            }) => {
                let seconds = i64::from(offset_minutes) * (PICOS_PER_MINUTE / PICOS_PER_SECOND);
                Self::local(i128::from(picos), seconds)
            }
            Value::Temporal(T::Timestamp(stamp)) => Self::local(stamp.as_picos(), 0),
            Value::Temporal(T::TimestampWithTimeZone { instant, zone }) => {
                Self::zoned(instant, zone)
            }
            _ => Err(ShiftError::ForeignValue),
        }
    }

    fn zoned(
        instant: novarocks_type_contract::TemporalMicros,
        zone: TimeZoneKey,
    ) -> Result<Self, ShiftError> {
        let source = TemporalZoneSource::Joda;
        let local = utc_to_local(zone, source, instant).map_err(|_| ShiftError::Arithmetic)?;
        let seconds = offset_seconds(zone, source, instant.floor_millis())
            .map_err(|_| ShiftError::Arithmetic)?;
        Self::local(local.as_picos(), i64::from(seconds))
    }

    fn local(picos: i128, offset_seconds: i64) -> Result<Self, ShiftError> {
        let days = picos.div_euclid(i128::from(PICOS_PER_DAY));
        let seconds = picos.rem_euclid(i128::from(PICOS_PER_DAY)) / i128::from(PICOS_PER_SECOND);
        Ok(Self {
            days: i32::try_from(days).map_err(|_| ShiftError::Arithmetic)?,
            seconds_of_day: seconds as i64,
            offset_seconds,
        })
    }
}
