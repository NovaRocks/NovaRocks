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

//! Trino 484 temporal text formats. The caller supplies the admitted destination.

use std::fmt::{self, Write};

use novarocks_type_contract::{
    IntervalField, IntervalQualifier, PICOS_PER_DAY, PICOS_PER_SECOND, TemporalMicros,
    TemporalPrecision, TemporalType, TemporalValue,
};

use crate::{
    datetime_value::date32_to_gregorian,
    temporal_zone_rules::{TemporalZoneSource, utc_to_local},
};

/// SQL CAST spells intervals using their qualifier. Client strings retain the
/// full day/clock or year/month shape, even for single-field intervals.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TemporalTextFormat {
    Cast,
    Trino {
        parametric_datetime: bool,
        parametric_interval: bool,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TemporalFormatError {
    InvalidValue,
    Range,
    Destination,
}

impl fmt::Display for TemporalFormatError {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str(match self {
            Self::InvalidValue => "Temporal value differs from its declaration",
            Self::Range => "Temporal value is outside the formatting range",
            Self::Destination => "Temporal text does not fit the formatting destination",
        })
    }
}

impl std::error::Error for TemporalFormatError {}

impl From<fmt::Error> for TemporalFormatError {
    fn from(_: fmt::Error) -> Self {
        Self::Destination
    }
}

pub fn format(
    out: &mut impl Write,
    ty: TemporalType,
    value: TemporalValue,
    form: TemporalTextFormat,
) -> Result<(), TemporalFormatError> {
    let value = value
        .validate(ty)
        .map_err(|_| TemporalFormatError::InvalidValue)?;
    let legacy_datetime = matches!(
        form,
        TemporalTextFormat::Trino {
            parametric_datetime: false,
            ..
        }
    );
    let precision = if legacy_datetime {
        TemporalPrecision::DEFAULT
    } else {
        ty.precision()
    };
    let display_timestamp = |value| {
        if legacy_datetime {
            super::round_timestamp(value, precision).map_err(|_| TemporalFormatError::Range)
        } else {
            Ok(value)
        }
    };
    let display_time = |value: i64| {
        if legacy_datetime {
            let step = precision.picosecond_step();
            ((value + step / 2) / step * step).rem_euclid(PICOS_PER_DAY)
        } else {
            value
        }
    };
    match value {
        TemporalValue::Time(picos) => write_clock(out, i128::from(display_time(picos)), precision)?,
        TemporalValue::TimeWithTimeZone {
            picos,
            offset_minutes,
        } => {
            write_clock(out, i128::from(display_time(picos)), precision)?;
            let sign = if offset_minutes < 0 { '-' } else { '+' };
            let minutes = offset_minutes.unsigned_abs();
            write!(out, "{sign}{:02}:{:02}", minutes / 60, minutes % 60)?;
        }
        TemporalValue::Timestamp(value) => {
            write_timestamp(out, display_timestamp(value)?, precision)?;
        }
        TemporalValue::TimestampWithTimeZone { instant, zone } => {
            let local = utc_to_local(zone, TemporalZoneSource::Jdk, display_timestamp(instant)?)
                .map_err(|_| TemporalFormatError::Range)?;
            write_timestamp(out, local, precision)?;
            write!(out, " {}", zone.name())?;
        }
        TemporalValue::IntervalYearMonth(months) => {
            let sign = if months < 0 { "-" } else { "" };
            let magnitude = months.unsigned_abs();
            match (form, ty) {
                (TemporalTextFormat::Cast, TemporalType::Interval(q))
                    if q.start() == IntervalField::Month =>
                {
                    write!(out, "{sign}{magnitude}")?;
                }
                (TemporalTextFormat::Cast, TemporalType::Interval(q))
                    if q.end() == IntervalField::Year =>
                {
                    write!(out, "{sign}{}", magnitude / 12)?;
                }
                _ => write!(out, "{sign}{}-{}", magnitude / 12, magnitude % 12)?,
            }
        }
        TemporalValue::IntervalDayTime(value) => {
            if let (TemporalTextFormat::Cast, TemporalType::Interval(qualifier)) = (form, ty) {
                return write_qualified_day_time(out, value, qualifier);
            }
            let (picos, precision) = match form {
                TemporalTextFormat::Trino {
                    parametric_interval: false,
                    ..
                } => (
                    i128::from(legacy_interval_millis(value)) * 1_000_000_000,
                    TemporalPrecision::DEFAULT,
                ),
                _ => (value.as_picos(), ty.precision()),
            };
            let sign = if picos < 0 { "-" } else { "" };
            let magnitude = picos.abs();
            let day = i128::from(PICOS_PER_DAY);
            write!(out, "{sign}{} ", magnitude / day)?;
            write_clock(out, magnitude % day, precision)?;
        }
    }
    Ok(())
}

fn legacy_interval_millis(value: TemporalMicros) -> i64 {
    // Do not multiply the rounded millis back into an i64-microseconds value:
    // the rounded rendering can exceed the interval's storage range.
    let micros = value.micros();
    micros.div_euclid(1000) + i64::from(micros.rem_euclid(1000) >= 500)
}

fn write_timestamp(
    out: &mut impl Write,
    value: TemporalMicros,
    precision: TemporalPrecision,
) -> Result<(), TemporalFormatError> {
    let picos = value.as_picos();
    let day = i128::from(PICOS_PER_DAY);
    let epoch_day = i32::try_from(picos.div_euclid(day)).map_err(|_| TemporalFormatError::Range)?;
    let (year, month, day_of_month) = date32_to_gregorian(epoch_day);
    if (0..=9999).contains(&year) {
        write!(out, "{year:04}")?;
    } else if year < 0 {
        write!(out, "-{:04}", year.unsigned_abs())?;
    } else {
        write!(out, "+{year}")?;
    }
    write!(out, "-{month:02}-{day_of_month:02} ")?;
    write_clock(out, picos.rem_euclid(day), precision)?;
    Ok(())
}

fn write_clock(out: &mut impl Write, picos: i128, precision: TemporalPrecision) -> fmt::Result {
    let per_second = i128::from(PICOS_PER_SECOND);
    let seconds = picos / per_second;
    let (hour, minute, second) = (seconds / 3600, seconds / 60 % 60, seconds % 60);
    write!(out, "{hour:02}:{minute:02}:{second:02}")?;
    write_fraction(out, picos % per_second, precision)
}

fn write_fraction(out: &mut impl Write, picos: i128, precision: TemporalPrecision) -> fmt::Result {
    if precision.digits() != 0 {
        let width = usize::from(precision.digits());
        let fraction = picos / i128::from(precision.picosecond_step());
        write!(out, ".{fraction:0width$}")?;
    }
    Ok(())
}

fn write_qualified_day_time(
    out: &mut impl Write,
    value: TemporalMicros,
    qualifier: IntervalQualifier,
) -> Result<(), TemporalFormatError> {
    use IntervalField::{Day, Hour, Minute, Second};
    let picos = value.as_picos();
    let magnitude = picos.abs();
    let per_second = i128::from(PICOS_PER_SECOND);
    let mut seconds = magnitude / per_second;
    if picos < 0 {
        out.write_char('-')?;
    }
    for (field, unit) in [(Day, 86_400), (Hour, 3600), (Minute, 60), (Second, 1)] {
        if field < qualifier.start() || field > qualifier.end() {
            continue;
        }
        let value = seconds / unit;
        seconds %= unit;
        if field == qualifier.start() {
            write!(out, "{value}")?;
        } else {
            let separator = if field == Hour { ' ' } else { ':' };
            write!(out, "{separator}{value:02}")?;
        }
    }
    write_fraction(
        out,
        magnitude % per_second,
        qualifier.fractional_precision(),
    )?;
    Ok(())
}
