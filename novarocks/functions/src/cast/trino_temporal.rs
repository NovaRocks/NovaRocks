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

//! Trino's unzoned microsecond casts. SQL admits the original represented
//! temporal types; precision and zoned type expansions belong to their owner.

use super::*;
use chrono::{Duration, LocalResult, NaiveDate, NaiveDateTime, TimeZone};
use chrono_tz::{GapInfo, Tz};
use regex::{Captures, Regex};
use std::sync::LazyLock;

const MICROS_PER_DAY: i64 = 86_400_000_000;

static TIMESTAMP: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(concat!(
        r"\A(?P<year>[-+]?[0-9]{4,})-(?P<month>[0-9]{1,2})-(?P<day>[0-9]{1,2})",
        r"(?: (?:(?P<hour>[0-9]{1,2}):(?P<minute>[0-9]{1,2})",
        r"(?::(?P<second>[0-9]{1,2})(?:\.(?P<fraction>[0-9]+))?)?)?",
        r"[ \t\n\r\x0b\x0c]*(?P<zone>[^\n\r\u{85}\u{2028}\u{2029}]+)?)?\z",
    ))
    .expect("static Trino timestamp grammar")
});

static TIME: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(concat!(
        r"\A(?P<hour>[0-9]{1,2}):(?P<minute>[0-9]{1,2})",
        r"(?::(?P<second>[0-9]{1,2})(?:\.(?P<fraction>[0-9]+))?)?",
        r"[ \t\n\r\x0b\x0c]*\z",
    ))
    .expect("static Trino time grammar")
});

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Operation {
    TextToTimestamp,
    TextToTime,
    TimestampToTime,
    TimestampNanosToMicros,
    TimestampToText,
    TimeToText(Option<u32>),
}

impl Operation {
    pub(super) fn may_raise(self) -> bool {
        match self {
            Self::TimestampToTime | Self::TimestampNanosToMicros => false,
            Self::TimeToText(limit) => limit.is_some_and(|len| len < 15),
            Self::TextToTimestamp | Self::TextToTime | Self::TimestampToText => true,
        }
    }

    pub(super) fn evaluate(
        self,
        array: &dyn Array,
        row: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<CastRowResult, super::trino::ConversionFailure> {
        use super::trino::ConversionFailure as E;
        let timestamp = || {
            array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .map(|array| array.value(row))
                .ok_or_else(|| invalid("cast timestamp source lost its checked carrier"))
        };
        let output = match self {
            Self::TextToTimestamp | Self::TextToTime => {
                let text = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| invalid("temporal cast text source lost its checked carrier"))?
                    .value(row);
                for _ in text.as_bytes().chunks(64) {
                    work.step()?;
                }
                work.flush()?;
                if self == Self::TextToTimestamp {
                    CastRowResult::Timestamp(parse_timestamp(text).ok_or(E::Invalid)?)
                } else {
                    CastRowResult::Signed(parse_time(text).ok_or(E::Invalid)?)
                }
            }
            Self::TimestampToTime => CastRowResult::Signed(timestamp()?.rem_euclid(MICROS_PER_DAY)),
            Self::TimestampNanosToMicros => {
                let value = array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| invalid("nanosecond cast source lost its checked carrier"))?
                    .value(row);
                CastRowResult::Timestamp(
                    value.div_euclid(1_000) + i64::from(value.rem_euclid(1_000) >= 500),
                )
            }
            Self::TimestampToText => {
                let micros = timestamp()?;
                let datetime = chrono::DateTime::from_timestamp_micros(micros).ok_or(E::Range)?;
                work.flush()?;
                CastRowResult::Text(datetime.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
            }
            Self::TimeToText(max_length) => {
                if max_length.is_some_and(|len| len < 15) {
                    return Err(E::Invalid);
                }
                let micros = array
                    .as_any()
                    .downcast_ref::<arrow_array::Time64MicrosecondArray>()
                    .ok_or_else(|| invalid("cast time source lost its checked carrier"))?
                    .value(row);
                if !(0..MICROS_PER_DAY).contains(&micros) {
                    return Err(invalid("TIME value is outside the authored civil day").into());
                }
                let seconds = micros / 1_000_000;
                let hour = seconds / 3600;
                let minute = seconds / 60 % 60;
                let second = seconds % 60;
                let fraction = micros % 1_000_000;
                work.flush()?;
                CastRowResult::Text(format!("{hour:02}:{minute:02}:{second:02}.{fraction:06}"))
            }
        };
        work.flush()?;
        Ok(output)
    }
}

fn number(fields: &Captures<'_>, name: &str) -> Option<u32> {
    fields
        .name(name)
        .map_or(Some(0), |n| n.as_str().parse().ok())
}

fn time_of_day(fields: &Captures<'_>) -> Option<(u32, u32, u32, i64)> {
    let hour = number(fields, "hour")?;
    let minute = number(fields, "minute")?;
    let second = number(fields, "second")?;
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let micros = if let Some(fraction) = fields.name("fraction") {
        let text = fraction.as_str();
        if text.len() > 12 {
            return None;
        }
        let value = text.parse::<i64>().ok()?;
        if text.len() > 6 {
            let divisor = 10_i64.pow((text.len() - 6) as u32);
            (value + divisor / 2) / divisor
        } else {
            value * 10_i64.pow((6 - text.len()) as u32)
        }
    } else {
        0
    };
    Some((hour, minute, second, micros))
}

fn parse_time(text: &str) -> Option<i64> {
    let fields = TIME.captures(text)?;
    let (hour, minute, second, micros) = time_of_day(&fields)?;
    Some((i64::from(hour * 3600 + minute * 60 + second) * 1_000_000 + micros) % MICROS_PER_DAY)
}

fn trino_trim(ch: char) -> bool {
    (ch.is_whitespace() && !matches!(ch, '\u{85}' | '\u{a0}' | '\u{2007}' | '\u{202f}'))
        || ('\u{1c}'..='\u{1f}').contains(&ch)
}

fn parse_timestamp(text: &str) -> Option<i64> {
    let fields = TIMESTAMP.captures(text.trim_matches(trino_trim))?;
    let year = fields.name("year")?.as_str().parse::<i32>().ok()?;
    let date = NaiveDate::from_ymd_opt(year, number(&fields, "month")?, number(&fields, "day")?)?;
    let (hour, minute, second, micros) = time_of_day(&fields)?;
    let mut local = date.and_hms_opt(hour, minute, second)?;
    if let Some(zone) = fields.name("zone") {
        local = resolve_local(zone.as_str(), local)?;
    }
    local.and_utc().timestamp_micros().checked_add(micros)
}

/// The CAST returns local fields, but validates the zone and advances over a
/// timezone gap just as ZonedDateTime.of does. An overlap keeps those fields.
fn resolve_local(zone: &str, local: NaiveDateTime) -> Option<NaiveDateTime> {
    if zone == "Z" {
        return Some(local);
    }
    let offset = zone
        .strip_prefix("UTC")
        .or_else(|| zone.strip_prefix("GMT"))
        .or_else(|| zone.strip_prefix("UT"))
        .unwrap_or(zone);
    if (offset.is_empty() && !zone.is_empty()) || fixed_offset(offset) {
        return Some(local);
    }
    let timezone = zone.parse::<Tz>().ok()?;
    if timezone.from_local_datetime(&local) != LocalResult::None {
        return Some(local);
    }
    let gap = GapInfo::new(&local, &timezone)?;
    let (start, _) = gap.begin?;
    let end = gap.end?.naive_local();
    local.checked_add_signed(Duration::seconds((end - start).num_seconds()))
}

fn fixed_offset(value: &str) -> bool {
    let Some(value) = value.strip_prefix(['-', '+']) else {
        return false;
    };
    let parts = match value.as_bytes() {
        [a] if a.is_ascii_digit() => return true,
        [a, b] => ([*a, *b], *b"00", *b"00"),
        [a, b, c, d] | [a, b, b':', c, d] => ([*a, *b], [*c, *d], *b"00"),
        [a, b, c, d, e, f] | [a, b, b':', c, d, b':', e, f] => ([*a, *b], [*c, *d], [*e, *f]),
        _ => return false,
    };
    let digits = |[tens, ones]: [u8; 2]| {
        (tens.is_ascii_digit() && ones.is_ascii_digit()).then(|| (tens - b'0') * 10 + (ones - b'0'))
    };
    matches!(
        (digits(parts.0), digits(parts.1), digits(parts.2)),
        (Some(0..=17), Some(0..=59), Some(0..=59)) | (Some(18), Some(0), Some(0))
    )
}
