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

use std::fmt::Write;

use novarocks_type_contract::{TemporalMicros, TemporalValue};

use super::date_field::iso_week_year;
use crate::{
    KernelFailure,
    datetime_value::{
        DateParseObservation, date32_to_gregorian, gregorian_epoch_day_from_valid_parts,
        parse_trino_date32_untrimmed_observed,
    },
    kernel_control::invalid,
    kernel_input::EvaluationCheckpoints,
    temporal_value::gregorian_epoch_day,
    temporal_zone_rules::{TemporalZoneSource, offset_seconds},
};

const MILLIS_PER_DAY: i64 = 86_400_000;
const MONTHS: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];
const WEEKDAYS: [&str; 7] = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
];
const HALFDAYS: [&str; 2] = ["AM", "PM"];

#[derive(Debug)]
pub(super) enum Failure {
    Value,
    Kernel(KernelFailure),
}

impl From<KernelFailure> for Failure {
    fn from(error: KernelFailure) -> Self {
        Self::Kernel(error)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Field {
    Year,
    WeekYear,
    Month,
    Week,
    DayOfYear,
    DayOfMonth,
    DayOfWeek,
    Halfday,
    Hour,
    HalfdayHour,
    Minute,
    Second,
    Millisecond,
}

#[derive(Clone, Copy)]
enum Token {
    Literal(char),
    Number(Field, usize, usize),
    TwoDigitYear,
    Text(Field, bool),
    Fraction,
}

fn visit_tokens(
    pattern: &str,
    mut consume: impl FnMut(Token) -> Result<(), Failure>,
) -> Result<(), Failure> {
    use Field as F;
    use Token as T;
    let number = T::Number;
    let mut chars = pattern.chars();
    let mut any = false;
    while let Some(c) = chars.next() {
        if c != '%' {
            consume(T::Literal(c))?;
            any = true;
            continue;
        }
        let Some(escape) = chars.next() else {
            break;
        };
        let token = match escape {
            'Y' => number(F::Year, 4, 4),
            'y' => T::TwoDigitYear,
            'x' => number(F::WeekYear, 4, 4),
            'm' => number(F::Month, 2, 2),
            'c' => number(F::Month, 1, 2),
            'b' => T::Text(F::Month, true),
            'M' => T::Text(F::Month, false),
            'v' => number(F::Week, 2, 2),
            'j' => number(F::DayOfYear, 3, 3),
            'd' => number(F::DayOfMonth, 2, 2),
            'e' => number(F::DayOfMonth, 1, 2),
            'a' => T::Text(F::DayOfWeek, true),
            'W' => T::Text(F::DayOfWeek, false),
            'p' => T::Text(F::Halfday, false),
            'H' => number(F::Hour, 2, 2),
            'k' => number(F::Hour, 1, 2),
            'h' | 'I' => number(F::HalfdayHour, 2, 2),
            'l' => number(F::HalfdayHour, 1, 2),
            'i' => number(F::Minute, 2, 2),
            's' | 'S' => number(F::Second, 2, 2),
            'f' => T::Fraction,
            'r' | 'T' => {
                let hour = if escape == 'r' {
                    F::HalfdayHour
                } else {
                    F::Hour
                };
                for token in [
                    number(hour, 2, 2),
                    T::Literal(':'),
                    number(F::Minute, 2, 2),
                    T::Literal(':'),
                    number(F::Second, 2, 2),
                ] {
                    consume(token)?;
                }
                any = true;
                if escape == 'T' {
                    continue;
                }
                consume(T::Literal(' '))?;
                T::Text(F::Halfday, false)
            }
            'w' | 'U' | 'u' | 'V' | 'X' | 'D' => return Err(Failure::Value),
            literal => T::Literal(literal),
        };
        consume(token)?;
        any = true;
    }
    if any { Ok(()) } else { Err(Failure::Value) }
}

pub(super) fn format(
    stamp: TemporalValue,
    pattern: &str,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<String, Failure> {
    let millis = match stamp {
        TemporalValue::Timestamp(value) => {
            let micros = value.micros();
            micros.div_euclid(1000) + i64::from(micros.rem_euclid(1000) >= 500)
        }
        TemporalValue::TimestampWithTimeZone { instant, zone } => {
            let millis = instant.floor_millis()
                + i64::from(instant.as_picos().rem_euclid(1_000_000_000) >= 500_000_000);
            let offset = offset_seconds(zone, TemporalZoneSource::Joda, millis)
                .map_err(|_| Failure::Value)?;
            millis
                .checked_add(i64::from(offset) * 1000)
                .ok_or(Failure::Value)?
        }
        _ => return Err(invalid("date_format expected an exact timestamp value").into()),
    };
    let stamp = CalendarMillis::from_millis(millis).ok_or(Failure::Value)?;
    let mut output = String::new();
    visit_tokens(pattern, |token| {
        work.step()?;
        match token {
            Token::Literal(value) => {
                reserve(&mut output, value.len_utf8())?;
                output.push(value);
            }
            Token::Number(field, width, _) => append_number(&mut output, stamp.get(field), width)?,
            Token::TwoDigitYear => append_number(&mut output, stamp.year.abs() % 100, 2)?,
            Token::Text(field, short) => {
                let names = field
                    .names()
                    .ok_or_else(|| invalid("unexpected textual calendar field"))?;
                let index = stamp.get(field) - field.first();
                let name = names.get(index as usize).ok_or(Failure::Value)?;
                let name = if short { &name[..3] } else { name };
                reserve(&mut output, name.len())?;
                output.push_str(name);
            }
            Token::Fraction => append_number(&mut output, stamp.get(Field::Millisecond) * 1000, 6)?,
        }
        Ok(())
    })?;
    Ok(output)
}

fn reserve(output: &mut String, bytes: usize) -> Result<(), KernelFailure> {
    let size = output
        .len()
        .checked_add(bytes)
        .ok_or(KernelFailure::ResourceExhausted)?;
    i32::try_from(size).map_err(|_| KernelFailure::ResourceExhausted)?;
    output
        .try_reserve(bytes)
        .map_err(|_| KernelFailure::ResourceExhausted)
}

fn append_number(output: &mut String, number: i64, width: usize) -> Result<(), KernelFailure> {
    // A signed i64 has at most twenty printed bytes. Reserve the actual width
    // so a valid string near its Arrow range does not hit a speculative bound.
    let magnitude = number.unsigned_abs();
    let digits = magnitude.checked_ilog10().map_or(1, |n| n as usize + 1);
    reserve(output, i64::from(number < 0) as usize + width.max(digits))?;
    if number < 0 {
        output.push('-');
    }
    write!(output, "{magnitude:0width$}").map_err(|_| invalid("writing date_format output failed"))
}

pub(super) fn parse(
    mut text: &str,
    pattern: &str,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<TemporalValue, Failure> {
    let millis = if pattern == "%Y-%m-%d" {
        let days = parse_trino_date32_untrimmed_observed(text, |event| match event {
            DateParseObservation::Step => work.step(),
            DateParseObservation::OpaqueBoundary => work.flush(),
        })?
        .ok_or(Failure::Value)?;
        i64::from(days) * MILLIS_PER_DAY
    } else {
        let mut saved: [Vec<(Field, i64)>; 12] = std::array::from_fn(|_| Vec::new());
        visit_tokens(pattern, |token| {
            work.step()?;
            if let Some((field, value)) = token.consume(&mut text).ok_or(Failure::Value)? {
                let bucket = &mut saved[field.rank()];
                bucket
                    .try_reserve(1)
                    .map_err(|_| KernelFailure::ResourceExhausted)?;
                bucket.push((field, value));
            }
            Ok(())
        })?;
        if !text.is_empty() {
            return Err(Failure::Value);
        }
        let initial_year =
            if !saved[0].is_empty() || !saved[1..6].iter().any(|bucket| !bucket.is_empty()) {
                1970
            } else {
                2000
            };
        let mut stamp = gregorian_epoch_day_from_valid_parts(initial_year, 1, 1) * MILLIS_PER_DAY;
        let total: usize = saved.iter().map(Vec::len).sum();
        // Preserve order for fields of equal size. On the first pass each
        // field also clears finer portions, and on the second only the last does.
        for reset_all in [true, false] {
            for (position, &(field, value)) in saved.iter().flatten().enumerate() {
                work.step()?;
                let current = CalendarMillis::from_millis(stamp).ok_or(Failure::Value)?;
                stamp = current
                    .apply(field, value, reset_all || position + 1 == total)
                    .ok_or(Failure::Value)?;
            }
        }
        stamp
    };
    let micros = millis.checked_mul(1000).ok_or(Failure::Value)?;
    Ok(TemporalValue::Timestamp(TemporalMicros::whole_micros(
        micros,
    )))
}

impl Token {
    fn consume(self, text: &mut &str) -> Option<Option<(Field, i64)>> {
        let value = match self {
            Self::Literal(literal) => {
                let first = text.chars().next()?;
                if first != literal && !first.eq_ignore_ascii_case(&literal) {
                    return None;
                }
                *text = &text[first.len_utf8()..];
                return Some(None);
            }
            Self::Number(field, _, limit) => {
                let signed = matches!(field, Field::Year | Field::WeekYear);
                let negative = signed && text.starts_with('-');
                if signed && (negative || text.starts_with('+')) {
                    *text = &text[1..];
                }
                let (value, _) = digits(text, limit)?;
                (field, if negative { -value } else { value })
            }
            Self::TwoDigitYear => {
                let (value, length) = digits(text, 2)?;
                if length != 2 {
                    return None;
                }
                (
                    Field::Year,
                    if value < 70 {
                        2000 + value
                    } else {
                        1900 + value
                    },
                )
            }
            Self::Text(field, _) => {
                let (value, bytes) = field
                    .names()?
                    .iter()
                    .enumerate()
                    .filter_map(|(index, name)| {
                        let matches = |value: &str| {
                            text.get(..value.len())
                                .is_some_and(|prefix| prefix.eq_ignore_ascii_case(value))
                        };
                        let bytes = if matches(name) {
                            name.len()
                        } else if name.len() >= 3 && matches(&name[..3]) {
                            3
                        } else {
                            return None;
                        };
                        Some((index as i64 + field.first(), bytes))
                    })
                    .max_by_key(|&(_, bytes)| bytes)?;
                *text = &text[bytes..];
                (field, value)
            }
            Self::Fraction => {
                let (digits, length) = digits(text, 9)?;
                let millis = if length <= 3 {
                    digits * 10_i64.pow((3 - length) as u32)
                } else {
                    digits / 10_i64.pow((length - 3) as u32)
                };
                (Field::Millisecond, millis)
            }
        };
        Some(Some(value))
    }
}

fn digits(text: &mut &str, limit: usize) -> Option<(i64, usize)> {
    let mut value = 0;
    let mut length = 0;
    for byte in text.bytes().take(limit).take_while(u8::is_ascii_digit) {
        value = value * 10 + i64::from(byte - b'0');
        length += 1;
    }
    *text = &text[length..];
    (length != 0).then_some((value, length))
}

impl Field {
    fn first(self) -> i64 {
        if self == Self::Halfday { 0 } else { 1 }
    }

    fn names(self) -> Option<&'static [&'static str]> {
        match self {
            Self::Month => Some(&MONTHS),
            Self::DayOfWeek => Some(&WEEKDAYS),
            Self::Halfday => Some(&HALFDAYS),
            _ => None,
        }
    }

    fn rank(self) -> usize {
        match self {
            Self::Year | Self::WeekYear => 0,
            Self::Month => 1,
            Self::Week => 2,
            Self::DayOfYear => 3,
            Self::DayOfMonth => 4,
            Self::DayOfWeek => 5,
            Self::Halfday => 6,
            Self::Hour => 7,
            Self::HalfdayHour => 8,
            Self::Minute => 9,
            Self::Second => 10,
            Self::Millisecond => 11,
        }
    }
}

struct CalendarMillis {
    date: i64,
    year: i64,
    month: i64,
    day: i64,
    time: i64,
}

impl CalendarMillis {
    fn from_millis(millis: i64) -> Option<Self> {
        let date = millis.div_euclid(MILLIS_PER_DAY);
        let (year, month, day) = date32_to_gregorian(i32::try_from(date).ok()?);
        Some(Self {
            date,
            year: i64::from(year),
            month: i64::from(month),
            day: i64::from(day),
            time: millis.rem_euclid(MILLIS_PER_DAY),
        })
    }

    fn get(&self, field: Field) -> i64 {
        match field {
            Field::Year => self.year,
            Field::WeekYear => iso_week_year(self.date, self.year).0,
            Field::Month => self.month,
            Field::Week => (self.date - iso_week_year(self.date, self.year).1) / 7 + 1,
            Field::DayOfYear => {
                self.date - gregorian_epoch_day_from_valid_parts(self.year, 1, 1) + 1
            }
            Field::DayOfMonth => self.day,
            Field::DayOfWeek => (self.date + 3).rem_euclid(7) + 1,
            Field::Halfday => self.time / 43_200_000,
            Field::Hour => self.time / 3_600_000,
            Field::HalfdayHour => (self.get(Field::Hour) + 11) % 12 + 1,
            Field::Minute => self.time / 60_000 % 60,
            Field::Second => self.time / 1000 % 60,
            Field::Millisecond => self.time % 1000,
        }
    }

    fn apply(self, field: Field, value: i64, clear_smaller: bool) -> Option<i64> {
        let jan1 = |year| gregorian_epoch_day_from_valid_parts(year, 1, 1);
        let week1 = |year| {
            let jan4 = jan1(year) + 3;
            jan4 - (jan4 + 3).rem_euclid(7)
        };
        let clip = |year: i64, month: i64, day: i64| {
            let first = gregorian_epoch_day(year, month, 1).ok()?;
            let next = if month == 12 {
                jan1(year + 1)
            } else {
                gregorian_epoch_day(year, month + 1, 1).ok()?
            };
            Some(first + day.min(next - first) - 1)
        };
        let calendar = |date: i64| date * MILLIS_PER_DAY;
        let carried = |date, floor| {
            calendar(if clear_smaller { floor } else { date })
                + if clear_smaller { 0 } else { self.time }
        };
        let clock = |scale, delta| {
            calendar(self.date) + (self.time + scale * delta)
                - if clear_smaller { self.time % scale } else { 0 }
        };
        Some(match field {
            Field::Year => carried(clip(value, self.month, self.day)?, jan1(value)),
            Field::WeekYear => {
                let start = week1(value);
                let maximum = (week1(value + 1) - start) / 7;
                let date = start
                    + (self.get(Field::Week).min(maximum) - 1) * 7
                    + self.get(Field::DayOfWeek)
                    - 1;
                carried(date, start)
            }
            Field::Month if (1..=12).contains(&value) => carried(
                clip(self.year, value, self.day)?,
                gregorian_epoch_day(self.year, value, 1).ok()?,
            ),
            Field::Week if value > 0 => {
                let (year, start) = iso_week_year(self.date, self.year);
                let epoch = start + (value - 1) * 7;
                if epoch >= week1(year + 1) {
                    return None;
                }
                carried(epoch + self.get(Field::DayOfWeek) - 1, epoch)
            }
            Field::DayOfYear if value > 0 => {
                let date = jan1(self.year) + value - 1;
                if date >= jan1(self.year + 1) {
                    return None;
                }
                carried(date, date)
            }
            Field::DayOfMonth => {
                let date = gregorian_epoch_day(self.year, self.month, value).ok()?;
                carried(date, date)
            }
            Field::DayOfWeek if (1..=7).contains(&value) => {
                let date = self.date + value - self.get(Field::DayOfWeek);
                carried(date, date)
            }
            Field::Halfday if (0..=1).contains(&value) => {
                clock(43_200_000, value - self.get(field))
            }
            Field::Hour if (0..24).contains(&value) => clock(3_600_000, value - self.get(field)),
            Field::HalfdayHour if (1..=12).contains(&value) => {
                clock(3_600_000, value % 12 - self.get(field) % 12)
            }
            Field::Minute | Field::Second if (0..60).contains(&value) => clock(
                if field == Field::Minute { 60_000 } else { 1000 },
                value - self.get(field),
            ),
            Field::Millisecond => clock(1, value - self.get(field)),
            _ => return None,
        })
    }
}
