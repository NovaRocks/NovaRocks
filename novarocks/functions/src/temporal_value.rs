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

use std::sync::LazyLock;

use crate::temporal_zone_rules::{JdkGapPolicy, ZoneRuleError, jdk_local_to_utc};
use novarocks_type_contract::{
    IntervalField, IntervalQualifier, PICOS_PER_DAY, PICOS_PER_SECOND, TemporalMicros,
    TemporalPrecision, TemporalType, TemporalValue, TimeZoneKey,
};
use regex::{Captures, Regex};

#[path = "temporal_text.rs"]
mod temporal_text;
pub use temporal_text::{TemporalFormatError, TemporalTextFormat, format};

const SECONDS_PER_DAY: i64 = 86_400;
const MICROS_PER_SECOND: i64 = 1_000_000;

// Trino 484 DateTimes.java DATETIME_PATTERN and TIME_PATTERN. Use ASCII digit and
// whitespace classes and Java's default dot exclusions for the region capture.
static TIMESTAMP: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(concat!(
        r"\A(?<year>[-+]?[0-9]{4,})-(?<month>[0-9]{1,2})-(?<day>[0-9]{1,2})",
        r"( (?:(?<hour>[0-9]{1,2}):(?<minute>[0-9]{1,2})(?::(?<second>[0-9]{1,2})(?:\.(?<fraction>[0-9]+))?)?)?",
        r"[ \t\n\x0B\x0C\r]*(?<zone>[^\r\n\u{0085}\u{2028}\u{2029}]+)?)?\z",
    ))
    .expect("static timestamp lexical grammar")
});

static TIME: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(concat!(
        r"\A(?<hour>[0-9]{1,2}):(?<minute>[0-9]{1,2})(?::(?<second>[0-9]{1,2})(?:\.(?<fraction>[0-9]+))?)?",
        r"[ \t\n\x0B\x0C\r]*(?<offset>[+-][0-9]{2}:[0-9]{2})?\z",
    ))
    .expect("static time lexical grammar")
});

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LiteralError {
    Syntax,
    InvalidDate,
    InvalidTime,
    Precision,
    Range,
    IntervalRange,
    NonexistentLocalTime,
    InvalidZone,
    DifferentTemporalFamilies,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParsedTemporal {
    pub ty: TemporalType,
    pub value: TemporalValue,
}

fn number(groups: &Captures<'_>, name: &str) -> Result<i64, LiteralError> {
    groups.name(name).map_or(Ok(0), |value| {
        value.as_str().parse().map_err(|_| LiteralError::Syntax)
    })
}

fn clock_seconds(groups: &Captures<'_>) -> Result<i64, LiteralError> {
    let hour = number(groups, "hour")?;
    let minute = number(groups, "minute")?;
    let second = number(groups, "second")?;
    if hour > 23 || minute > 59 || second > 59 {
        return Err(LiteralError::InvalidTime);
    }
    Ok(hour * 3600 + minute * 60 + second)
}

fn clock_parts(groups: &Captures<'_>) -> Result<(i64, TemporalPrecision, i64), LiteralError> {
    let seconds = clock_seconds(groups)?;
    let fraction = groups.name("fraction").map_or("", |m| m.as_str());
    let digits = u8::try_from(fraction.len()).map_err(|_| LiteralError::Precision)?;
    let precision = TemporalPrecision::new(digits).map_err(|_| LiteralError::Precision)?;
    let units: i64 = if fraction.is_empty() {
        0
    } else {
        fraction.parse().map_err(|_| LiteralError::Syntax)?
    };
    Ok((seconds, precision, units * precision.picosecond_step()))
}

impl std::fmt::Display for LiteralError {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::Syntax => "invalid temporal syntax",
            Self::InvalidDate => "invalid calendar date",
            Self::InvalidTime => "invalid clock time",
            Self::Precision => "invalid temporal precision",
            Self::Range => "temporal value out of range",
            Self::IntervalRange => "interval field overflow",
            Self::NonexistentLocalTime => "local time falls in a time zone gap",
            Self::InvalidZone => "invalid time zone",
            Self::DifferentTemporalFamilies => "incompatible temporal types",
        };
        out.write_str(message)
    }
}

/// Parse the exact interval grammar for the selected endpoints. The shorter
/// forms end at a field boundary and contribute zero for omitted fields.
pub fn parse_interval(
    text: &str,
    qualifier: IntervalQualifier,
    outer_negative: bool,
) -> Result<TemporalValue, LiteralError> {
    use IntervalField::*;
    let (negative, text) = text
        .strip_prefix('-')
        .map_or((false, text), |rest| (true, rest));
    let negative = negative ^ outer_negative;
    let mut rest = text.strip_prefix('+').unwrap_or(text);
    if rest.starts_with(['+', '-']) {
        return Err(LiteralError::Syntax);
    }
    let mut field = qualifier.start();
    let mut total = 0i128;
    loop {
        let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
        let (integer, tail) = rest.split_at(digits);
        let magnitude: i64 = integer.parse().map_err(|_| {
            if integer.is_empty() {
                LiteralError::Syntax
            } else {
                LiteralError::Range
            }
        })?;
        if (field != qualifier.start() || qualifier.is_year_month())
            && magnitude > i64::from(i32::MAX)
        {
            return Err(LiteralError::Range);
        }
        let factor = match field {
            Year => 12,
            Month => 1,
            Day => 86_400 * i128::from(PICOS_PER_SECOND),
            Hour => 3_600 * i128::from(PICOS_PER_SECOND),
            Minute => 60 * i128::from(PICOS_PER_SECOND),
            Second => i128::from(PICOS_PER_SECOND),
        };
        total += factor * i128::from(magnitude);
        if tail.is_empty() {
            break;
        }
        if field == Second {
            let fraction = tail.strip_prefix('.').ok_or(LiteralError::Syntax)?;
            total += rounded_fraction(fraction, qualifier.fractional_precision(), negative)?;
            break;
        }
        if field == qualifier.end() {
            return Err(LiteralError::Syntax);
        }
        let (separator, next) = match field {
            Year => ('-', Month),
            Day => (' ', Hour),
            Hour => (':', Minute),
            Minute => (':', Second),
            Month | Second => return Err(LiteralError::Syntax),
        };
        rest = tail.strip_prefix(separator).ok_or(LiteralError::Syntax)?;
        field = next;
    }
    if negative {
        total = -total;
    }
    if qualifier.is_year_month() {
        return i32::try_from(total)
            .map(TemporalValue::IntervalYearMonth)
            .map_err(|_| LiteralError::Range);
    }
    TemporalMicros::from_picos(total)
        .map(TemporalValue::IntervalDayTime)
        .ok_or(LiteralError::Range)
}

pub fn interval_leading_magnitude(value: TemporalValue, field: IntervalField) -> Option<u64> {
    use IntervalField::*;
    Some(match (value, field) {
        (TemporalValue::IntervalYearMonth(months), Year) => u64::from(months.unsigned_abs() / 12),
        (TemporalValue::IntervalYearMonth(months), Month) => u64::from(months.unsigned_abs()),
        (TemporalValue::IntervalDayTime(value), Day | Hour | Minute | Second) => {
            let unit_seconds = match field {
                Day => 86_400,
                Hour => 3_600,
                Minute => 60,
                Second => 1,
                _ => unreachable!("day-time leading field"),
            };
            let magnitude =
                value.as_picos().unsigned_abs() / (unit_seconds * PICOS_PER_SECOND as u128);
            u64::try_from(magnitude).ok()?
        }
        _ => return None,
    })
}

fn rounded_fraction(
    fraction: &str,
    precision: TemporalPrecision,
    negative: bool,
) -> Result<i128, LiteralError> {
    if !fraction.bytes().all(|digit| digit.is_ascii_digit()) {
        return Err(LiteralError::Syntax);
    }
    let digits = usize::from(precision.digits());
    let mut units = 0i128;
    let mut tail = fraction.bytes().map(|digit| digit - b'0');
    for _ in 0..digits {
        units = units * 10 + i128::from(tail.next().unwrap_or(0));
    }
    if let Some(digit) = tail.next()
        && (digit > 5 || (digit == 5 && (!negative || tail.any(|next| next != 0))))
    {
        units += 1;
    }
    Ok(units * i128::from(precision.picosecond_step()))
}

/// DATE literals use the original observed Trino Date32 author.
pub fn parse_date_literal_observed(
    text: &str,
    mut step: impl FnMut() -> Result<(), novarocks_type_contract::CompileControlError>,
) -> Result<Option<i32>, novarocks_type_contract::CompileControlError> {
    crate::datetime_value::parse_trino_date32_observed(text, |_| step())
}

pub fn parse_time_literal(text: &str) -> Result<ParsedTemporal, LiteralError> {
    let groups = TIME.captures(text).ok_or(LiteralError::Syntax)?;
    let (seconds, precision, fraction) = clock_parts(&groups)?;
    let picos = seconds * PICOS_PER_SECOND + fraction;
    let Some(offset) = groups.name("offset") else {
        return checked(TemporalType::Time(precision), TemporalValue::Time(picos));
    };
    let key = TimeZoneKey::parse(offset.as_str()).ok_or(LiteralError::InvalidZone)?;
    let offset_minutes = key
        .fixed_offset_minutes()
        .ok_or(LiteralError::InvalidZone)?;
    checked(
        TemporalType::TimeWithTimeZone(precision),
        TemporalValue::TimeWithTimeZone {
            picos,
            offset_minutes,
        },
    )
}

pub(crate) fn parse_zoned_time_cast(
    text: &str,
    precision: TemporalPrecision,
) -> Result<(i64, Option<i16>), LiteralError> {
    let groups = TIME.captures(text).ok_or(LiteralError::Syntax)?;
    let seconds = clock_seconds(&groups)?;
    let fraction = groups.name("fraction").map_or("", |value| value.as_str());
    let picos = (i128::from(seconds) * i128::from(PICOS_PER_SECOND)
        + rounded_fraction(fraction, precision, false)?)
    .rem_euclid(i128::from(PICOS_PER_DAY)) as i64;
    let offset = groups
        .name("offset")
        .map(|offset| {
            TimeZoneKey::parse(offset.as_str())
                .and_then(TimeZoneKey::fixed_offset_minutes)
                .ok_or(LiteralError::InvalidZone)
        })
        .transpose()?;
    Ok((picos, offset))
}

pub fn parse_timestamp_literal(text: &str) -> Result<ParsedTemporal, LiteralError> {
    parse_timestamp(text, None)
}

/// String conversions resolve gaps with java.time's forward shift and can
/// supply a session zone when no zone is present in a zoned target.
pub(crate) fn parse_timestamp_cast(
    text: &str,
    target: TemporalType,
    session_zone: Option<TimeZoneKey>,
) -> Result<ParsedTemporal, LiteralError> {
    parse_timestamp(text, Some((target, session_zone)))
}

fn parse_timestamp(
    text: &str,
    cast: Option<(TemporalType, Option<TimeZoneKey>)>,
) -> Result<ParsedTemporal, LiteralError> {
    let groups = TIMESTAMP.captures(text).ok_or(LiteralError::Syntax)?;
    let day = gregorian_epoch_day(
        number(&groups, "year")?,
        number(&groups, "month")?,
        number(&groups, "day")?,
    )?;
    let (seconds, precision, fraction) = if let Some((target, _)) = cast {
        let seconds = clock_seconds(&groups)?;
        let precision = target.precision();
        let fraction = groups.name("fraction").map_or("", |m| m.as_str());
        let fraction = rounded_fraction(fraction, precision, false)? as i64;
        (seconds, precision, fraction)
    } else {
        clock_parts(&groups)?
    };
    let epoch_seconds = day * SECONDS_PER_DAY + seconds;
    let wall_picos = i128::from(epoch_seconds) * i128::from(PICOS_PER_SECOND);
    let raw_zone = groups.name("zone").map(|m| m.as_str());
    let session_zone = cast.and_then(|(target, zone)| {
        matches!(target, TemporalType::TimestampWithTimeZone(_))
            .then_some(zone)
            .flatten()
    });
    let Some(raw_zone) = raw_zone.or_else(|| session_zone.map(TimeZoneKey::name)) else {
        // The pinned literal parser checks the seconds-to-micros product before
        // applying the fraction. From normalized carriers, all i64 micros are
        // representable. Here preserve that separate lexical conversion check.
        epoch_seconds
            .checked_mul(MICROS_PER_SECOND)
            .ok_or(LiteralError::Range)?;
        let wall = TemporalMicros::from_picos(wall_picos + i128::from(fraction))
            .ok_or(LiteralError::Range)?;
        return checked(
            TemporalType::Timestamp(precision),
            TemporalValue::Timestamp(wall),
        );
    };

    // The raw input determines Java's lookup rules; the Trino key determines
    // the stored/rendered value identity. Their normalization is independent.
    let unzoned = cast.is_some_and(|(target, _)| matches!(target, TemporalType::Timestamp(_)));
    let lookup = TimeZoneKey::parse(raw_zone);
    let zone = if unzoned {
        java_offset_seconds(raw_zone)
            .or_else(|| {
                ["UTC", "GMT", "UT"]
                    .into_iter()
                    .find_map(|prefix| raw_zone.strip_prefix(prefix).and_then(java_offset_seconds))
            })
            .map(LiteralZone::FixedOffset)
            .map_or_else(
                || literal_zone(raw_zone, lookup.ok_or(LiteralError::InvalidZone)?),
                Ok,
            )?
    } else {
        literal_zone(raw_zone, lookup.ok_or(LiteralError::InvalidZone)?)?
    };
    let key = lookup.unwrap_or(TimeZoneKey::UTC);
    let gap = if cast.is_some() {
        JdkGapPolicy::Advance
    } else {
        JdkGapPolicy::Reject
    };
    let instant = match zone {
        LiteralZone::FixedOffset(seconds) => {
            let shift = if unzoned { 0 } else { seconds };
            TemporalMicros::from_picos(
                wall_picos - i128::from(shift) * i128::from(PICOS_PER_SECOND),
            )
            .ok_or(LiteralError::Range)?
        }
        LiteralZone::IndexedRegion => {
            use crate::temporal_zone_rules::{TemporalZoneSource, utc_to_local};
            let wall = TemporalMicros::from_picos(wall_picos).ok_or(LiteralError::Range)?;
            let instant = jdk_local_to_utc(key, wall, gap).map_err(|cause| match cause {
                ZoneRuleError::NonexistentLocalTime => LiteralError::NonexistentLocalTime,
                ZoneRuleError::OutOfRange => LiteralError::Range,
            })?;
            if unzoned {
                // Trino projects the normalized local clock after the JDK
                // transition. Only the gap can shift this wall-clock value.
                utc_to_local(key, TemporalZoneSource::Jdk, instant)
                    .map_err(|_| LiteralError::Range)?
            } else {
                instant
            }
        }
    };
    let instant = TemporalMicros::from_picos(instant.as_picos() + i128::from(fraction))
        .ok_or(LiteralError::Range)?;
    if unzoned {
        checked(
            TemporalType::Timestamp(precision),
            TemporalValue::Timestamp(instant),
        )
    } else {
        checked(
            TemporalType::TimestampWithTimeZone(precision),
            TemporalValue::TimestampWithTimeZone { instant, zone: key },
        )
    }
}

pub(crate) fn checked(
    ty: TemporalType,
    value: TemporalValue,
) -> Result<ParsedTemporal, LiteralError> {
    let value = value.validate(ty).map_err(|_| LiteralError::Range)?;
    Ok(ParsedTemporal { ty, value })
}

enum LiteralZone {
    FixedOffset(i32),
    IndexedRegion,
}

fn literal_zone(raw: &str, key: TimeZoneKey) -> Result<LiteralZone, LiteralError> {
    if raw == key.name() {
        return Ok(match key.fixed_offset_minutes() {
            Some(minutes) => LiteralZone::FixedOffset(i32::from(minutes) * 60),
            None => LiteralZone::IndexedRegion,
        });
    }

    if let Some(seconds) = java_offset_seconds(raw) {
        return Ok(LiteralZone::FixedOffset(seconds));
    }

    for prefix in ["UTC", "GMT", "UT"] {
        if let Some(suffix) = raw.strip_prefix(prefix) {
            if suffix.is_empty() {
                return Ok(LiteralZone::FixedOffset(0));
            }
            if let Some(seconds) = java_offset_seconds(suffix) {
                return Ok(LiteralZone::FixedOffset(seconds));
            }
        }
    }

    // These region spellings are common IANA JDK zero-offset aliases. The
    // Trino index needs no separate rule object because the keys are zero.
    if matches!(
        raw,
        "Z" | "Zulu"
            | "UCT"
            | "Greenwich"
            | "Universal"
            | "GMT0"
            | "Etc/GMT"
            | "Etc/GMT0"
            | "Etc/GMT+0"
            | "Etc/GMT-0"
            | "Etc/UTC"
            | "Etc/Universal"
            | "Etc/UCT"
    ) {
        return Ok(LiteralZone::FixedOffset(0));
    }

    if let Some(suffix) = raw.strip_prefix("Etc/GMT") {
        if let Some((sign, magnitude)) = suffix.as_bytes().split_first() {
            let fixed_hours = match (sign, magnitude) {
                (b'+', [b'1', b'0'..=b'2'] | [b'1'..=b'9'])
                | (b'-', [b'1', b'0'..=b'4'] | [b'1'..=b'9']) => suffix.parse::<i32>().ok(),
                _ => None,
            };
            if let Some(hour) = fixed_hours {
                return Ok(LiteralZone::FixedOffset(-hour * 3600));
            }
        }
    }
    Err(LiteralError::InvalidZone)
}

fn java_offset_seconds(raw: &str) -> Option<i32> {
    fn digit(value: u8) -> Option<i32> {
        value.is_ascii_digit().then(|| i32::from(value - b'0'))
    }
    fn pair(a: u8, b: u8) -> Option<i32> {
        Some(digit(a)? * 10 + digit(b)?)
    }

    let (&sign, offset) = raw.as_bytes().split_first()?;
    let sign = match sign {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    let (hour, minute, second) = match offset {
        &[a] => (digit(a)?, 0, 0),
        &[a, b] => (pair(a, b)?, 0, 0),
        &[a, b, c, d] | &[a, b, b':', c, d] => (pair(a, b)?, pair(c, d)?, 0),
        &[a, b, c, d, e, f] | &[a, b, b':', c, d, b':', e, f] => {
            (pair(a, b)?, pair(c, d)?, pair(e, f)?)
        }
        _ => return None,
    };
    let seconds = hour * 3600 + minute * 60 + second;
    (minute < 60 && second < 60 && seconds <= 18 * 3600).then_some(sign * seconds)
}

/// Signed Gregorian date in JDK LocalDate's full range. No Chrono boundary.
pub fn gregorian_epoch_day(year: i64, month: i64, day: i64) -> Result<i64, LiteralError> {
    if !(-999_999_999..=999_999_999).contains(&year) {
        return Err(LiteralError::InvalidDate);
    }
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let month_days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return Err(LiteralError::InvalidDate),
    };
    if !(1..=month_days).contains(&day) {
        return Err(LiteralError::InvalidDate);
    }
    Ok(crate::datetime_value::gregorian_epoch_day_from_valid_parts(
        year, month, day,
    ))
}

pub fn date_epoch_day(year: i64, month: i64, day: i64) -> Result<i32, LiteralError> {
    gregorian_epoch_day(year, month, day)
        .and_then(|days| i32::try_from(days).map_err(|_| LiteralError::Range))
}

pub fn date_to_timestamp(
    days: i32,
    precision: TemporalPrecision,
) -> Result<ParsedTemporal, LiteralError> {
    let micros = i64::from(days)
        .checked_mul(SECONDS_PER_DAY * MICROS_PER_SECOND)
        .ok_or(LiteralError::Range)?;
    checked(
        TemporalType::Timestamp(precision),
        TemporalValue::Timestamp(TemporalMicros::whole_micros(micros)),
    )
}

/// Coordinate a single precision round with a caller choosing the original
/// overload stage. This does not decide where a cross-zone cast must round.
fn round_half_up(picos: i128, target: TemporalPrecision) -> Option<i128> {
    let step = i128::from(target.picosecond_step());
    let (mut whole, fraction) = (picos.div_euclid(step), picos.rem_euclid(step));
    if fraction >= step - step / 2 {
        whole += 1;
    }
    whole.checked_mul(step)
}

pub fn round_timestamp(
    value: TemporalMicros,
    precision: TemporalPrecision,
) -> Result<TemporalMicros, LiteralError> {
    round_half_up(value.as_picos(), precision)
        .and_then(TemporalMicros::from_picos)
        .ok_or(LiteralError::Range)
}

pub(crate) fn round_time(picos: i64, precision: TemporalPrecision) -> Result<i64, LiteralError> {
    let rounded = round_half_up(i128::from(picos), precision).ok_or(LiteralError::Range)?;
    // Rounded time-of-day may be exactly a day. Reduce it before validation.
    i64::try_from(rounded.rem_euclid(i128::from(PICOS_PER_DAY))).map_err(|_| LiteralError::Range)
}

/// One same-family time/timestamp precision cast (no cross-family rules).
pub fn cast_precision(
    source: ParsedTemporal,
    target: TemporalType,
) -> Result<ParsedTemporal, LiteralError> {
    source
        .value
        .validate(source.ty)
        .map_err(|_| LiteralError::Range)?;
    let value = match (source.value, target) {
        (TemporalValue::Time(picos), TemporalType::Time(p)) => {
            TemporalValue::Time(round_time(picos, p)?)
        }
        (
            TemporalValue::TimeWithTimeZone {
                picos,
                offset_minutes,
            },
            TemporalType::TimeWithTimeZone(p),
        ) => TemporalValue::TimeWithTimeZone {
            picos: round_time(picos, p)?,
            offset_minutes,
        },
        (TemporalValue::Timestamp(value), TemporalType::Timestamp(p)) => {
            TemporalValue::Timestamp(round_timestamp(value, p)?)
        }
        (
            TemporalValue::TimestampWithTimeZone { instant, zone },
            TemporalType::TimestampWithTimeZone(p),
        ) => TemporalValue::TimestampWithTimeZone {
            instant: round_timestamp(instant, p)?,
            zone,
        },
        _ => return Err(LiteralError::DifferentTemporalFamilies),
    };
    checked(target, value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unzoned_text_cast_preserves_clock_and_projects_dst_gaps() {
        let ty = TemporalType::Timestamp(TemporalPrecision::PICOSECONDS);
        let extended = "-270000-01-01 12:34:56.123456789012";
        let local = parse_timestamp_literal(extended).unwrap().value;
        let named = format!("{extended} America/New_York");
        assert_eq!(parse_timestamp_cast(&named, ty, None).unwrap().value, local);
        let fixed = parse_timestamp_cast("2024-01-01 00:00:00 +18:00", ty, None)
            .unwrap()
            .value;
        assert_eq!(
            fixed,
            parse_timestamp_cast("2024-01-01 00:00:00", ty, None)
                .unwrap()
                .value,
        );
        let gap = parse_timestamp_cast("2024-03-10 02:30:00 America/New_York", ty, None)
            .unwrap()
            .value;
        assert_eq!(
            gap,
            parse_timestamp_cast("2024-03-10 03:30:00", ty, None)
                .unwrap()
                .value,
        );
    }

    fn zoned_utc(text: &str) -> TemporalMicros {
        match parse_timestamp_literal(text)
            .expect("valid zoned Trino reference literal")
            .value
        {
            TemporalValue::TimestampWithTimeZone { instant, .. } => instant,
            value => panic!("expected a zoned timestamp, got {value:?}"),
        }
    }

    #[test]
    fn precision_includes_written_trailing_zeros() {
        let parsed = parse_timestamp_literal("2024-01-01 00:00:00.1200").unwrap();
        assert_eq!(
            parsed.ty,
            TemporalType::Timestamp(TemporalPrecision::new(4).unwrap())
        );
        assert_eq!(
            parse_time_literal("12:00:00.123456789012 +05:30").unwrap(),
            ParsedTemporal {
                ty: TemporalType::TimeWithTimeZone(TemporalPrecision::PICOSECONDS),
                value: TemporalValue::TimeWithTimeZone {
                    picos: 43_200_123_456_789_012,
                    offset_minutes: 330,
                },
            }
        );
        assert_eq!(
            parse_timestamp_literal("1970-01-01 00:00:00.1234567890123"),
            Err(LiteralError::Precision)
        );
    }

    #[test]
    fn time_literal_rejects_named_or_out_of_range_offsets() {
        assert_eq!(
            parse_time_literal("12:00:00 UTC"),
            Err(LiteralError::Syntax)
        );
        assert_eq!(
            parse_time_literal("12:00:00 +14:01"),
            Err(LiteralError::InvalidZone)
        );
        assert!(parse_time_literal("12:00:00 -14:00").is_ok());
        assert_eq!(
            parse_time_literal("24:00:00"),
            Err(LiteralError::InvalidTime)
        );
    }

    #[test]
    fn jdk_gap_rejects_and_overlap_uses_first_offset() {
        assert_eq!(
            parse_timestamp_literal("2024-03-10 02:30:00 America/New_York"),
            Err(LiteralError::NonexistentLocalTime)
        );
        assert_eq!(
            zoned_utc("2024-11-03 01:30:00 America/New_York"),
            zoned_utc("2024-11-03 05:30:00 UTC")
        );
        assert_eq!(
            zoned_utc("2100-07-01 12:00:00 America/New_York"),
            zoned_utc("2100-07-01 16:00:00 UTC")
        );
        assert_eq!(
            zoned_utc("1900-01-01 00:00:00 Africa/Accra"),
            zoned_utc("1900-01-01 00:16:08 UTC")
        );
    }

    #[test]
    fn region_identity_survives_equal_utc_instant() {
        let a = parse_timestamp_literal("2024-01-01 00:00:00 America/New_York").unwrap();
        let b = parse_timestamp_literal("2024-01-01 00:00:00 US/Eastern").unwrap();
        assert_eq!(a.value.relation_key(a.ty), b.value.relation_key(b.ty));
        assert_ne!(a.value, b.value);
    }

    #[test]
    fn gregorian_math_covers_beyond_chrono_years() {
        assert_eq!(date_epoch_day(1970, 1, 1), Ok(0));
        assert_eq!(date_epoch_day(1969, 12, 31), Ok(-1));
        assert_eq!(date_epoch_day(2000, 2, 29), Ok(11_016));
        assert_eq!(date_epoch_day(1900, 2, 29), Err(LiteralError::InvalidDate));
        for year in [-280_000, 280_000] {
            let days = date_epoch_day(year, 1, 1).unwrap();
            let literal = parse_timestamp_literal(&format!("{year}-01-01")).unwrap();
            assert_eq!(
                literal,
                date_to_timestamp(days, TemporalPrecision::WHOLE_SECONDS).unwrap()
            );
        }
    }

    #[test]
    fn negative_ties_are_rounded_toward_positive_infinity() {
        let before = parse_timestamp_literal("1969-12-31 23:59:59.9999995").unwrap();
        let result = cast_precision(
            before,
            TemporalType::Timestamp(TemporalPrecision::MICROSECONDS),
        )
        .unwrap();
        assert_eq!(
            result.value,
            TemporalValue::Timestamp(TemporalMicros::whole_micros(0))
        );
        assert_eq!(
            round_timestamp(
                TemporalMicros::new(-2, 499_999).unwrap(),
                TemporalPrecision::MICROSECONDS
            )
            .unwrap()
            .micros(),
            -2
        );
        assert_eq!(
            round_timestamp(
                TemporalMicros::new(-2, 500_000).unwrap(),
                TemporalPrecision::MICROSECONDS
            )
            .unwrap()
            .micros(),
            -1
        );
    }

    #[test]
    fn time_precision_carry_wraps_and_retains_offset() {
        let source = parse_time_literal("23:59:59.9999 +05:30").unwrap();
        assert_eq!(
            cast_precision(
                source,
                TemporalType::TimeWithTimeZone(TemporalPrecision::DEFAULT)
            )
            .unwrap()
            .value,
            TemporalValue::TimeWithTimeZone {
                picos: 0,
                offset_minutes: 330
            }
        );
    }

    #[test]
    fn precision_cast_checks_both_signed_micros_extremes() {
        let min = TemporalMicros::whole_micros(i64::MIN);
        assert_eq!(
            round_timestamp(min, TemporalPrecision::PICOSECONDS),
            Ok(min)
        );
        let max = TemporalMicros::whole_micros(i64::MAX);
        assert_eq!(
            round_timestamp(max, TemporalPrecision::PICOSECONDS),
            Ok(max)
        );
        let next = TemporalMicros::new(i64::MAX, 500_000).unwrap();
        assert_eq!(
            round_timestamp(next, TemporalPrecision::MICROSECONDS),
            Err(LiteralError::Range)
        );
    }
}
