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

//! Pure date and timestamp value conversions shared by CPU function owners.
//! The accepted formats and invalid-value `None` behavior are retained from
//! the existing execution date helpers. Parsing library work is not a resource grant.

use arrow_schema::TimeUnit;
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, Timelike, Utc};

pub const UNIX_EPOCH_DAY_OFFSET: i32 = 719163; // 1970-01-01 in Julian days

/// Original v1 Date32 projection, shared without changing its epoch arithmetic.
pub fn naive_to_date32(date: NaiveDate) -> i32 {
    date.num_days_from_ce() - UNIX_EPOCH_DAY_OFFSET
}

pub fn date32_to_naive(days: i32) -> Option<NaiveDate> {
    NaiveDate::from_num_days_from_ce_opt(UNIX_EPOCH_DAY_OFFSET.checked_add(days)?)
}

pub fn timestamp_to_naive(unit: &TimeUnit, value: i64) -> Option<NaiveDateTime> {
    match unit {
        TimeUnit::Second => DateTime::<Utc>::from_timestamp(value, 0).map(|dt| dt.naive_utc()),
        TimeUnit::Millisecond => {
            let secs = value.div_euclid(1_000);
            let nanos = (value.rem_euclid(1_000) as u32) * 1_000_000;
            DateTime::<Utc>::from_timestamp(secs, nanos).map(|dt| dt.naive_utc())
        }
        TimeUnit::Microsecond => {
            let secs = value.div_euclid(1_000_000);
            let nanos = (value.rem_euclid(1_000_000) as u32) * 1_000;
            DateTime::<Utc>::from_timestamp(secs, nanos).map(|dt| dt.naive_utc())
        }
        TimeUnit::Nanosecond => {
            let secs = value.div_euclid(1_000_000_000);
            let nanos = value.rem_euclid(1_000_000_000) as u32;
            DateTime::<Utc>::from_timestamp(secs, nanos).map(|dt| dt.naive_utc())
        }
    }
}

pub fn naive_to_timestamp_micros(dt: NaiveDateTime) -> i64 {
    dt.and_utc().timestamp_micros()
}

/// Actual completed parser work and boundaries of opaque standard/library calls.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DateParseObservation {
    Step,
    OpaqueBoundary,
}

fn unobserved<T>(result: Result<T, std::convert::Infallible>) -> T {
    match result {
        Ok(value) => value,
        Err(never) => match never {},
    }
}

pub fn parse_date(s: &str) -> Option<NaiveDate> {
    unobserved(parse_date_observed(s, |_| Ok(())))
}

pub(crate) fn parse_date_observed<E>(
    s: &str,
    mut observe: impl FnMut(DateParseObservation) -> Result<(), E>,
) -> Result<Option<NaiveDate>, E> {
    for format in ["%Y-%m-%d", "%Y%m%d"] {
        observe(DateParseObservation::OpaqueBoundary)?;
        let parsed = NaiveDate::parse_from_str(s, format);
        observe(DateParseObservation::OpaqueBoundary)?;
        if let Ok(date) = parsed {
            return Ok(Some(date));
        }
    }
    Ok(None)
}

fn parse_datetime_flexible<E>(
    raw: &str,
    observe: &mut impl FnMut(DateParseObservation) -> Result<(), E>,
) -> Result<Option<NaiveDateTime>, E> {
    observe(DateParseObservation::OpaqueBoundary)?;
    let text = raw.trim();
    observe(DateParseObservation::OpaqueBoundary)?;
    let bytes = text.as_bytes();
    let invalid_start = bytes.is_empty() || !bytes[0].is_ascii_digit();
    observe(DateParseObservation::Step)?;
    if invalid_start {
        return Ok(None);
    }

    let mut pos = 0usize;
    while pos < bytes.len() && (bytes[pos].is_ascii_digit() || bytes[pos] == b'T') {
        pos += 1;
        observe(DateParseObservation::Step)?;
    }
    let mut compact_digits = 0usize;
    for byte in &bytes[..pos] {
        compact_digits += usize::from(byte.is_ascii_digit());
        observe(DateParseObservation::Step)?;
    }
    let is_compact = pos == bytes.len() || bytes.get(pos) == Some(&b'.');
    let mut field_len = if is_compact {
        if compact_digits == 4 || compact_digits == 8 || compact_digits >= 14 {
            4usize
        } else {
            2usize
        }
    } else {
        4usize
    };

    let mut values = [0u32; 7];
    let mut lengths = [0usize; 7];
    let mut field_idx = 0usize;
    let mut ptr = 0usize;
    while ptr < bytes.len() && bytes[ptr].is_ascii_digit() && field_idx < 7 {
        let start = ptr;
        let mut value = 0u32;
        let scan_to_delim = !is_compact && field_idx != 6;
        while ptr < bytes.len() && bytes[ptr].is_ascii_digit() && (scan_to_delim || field_len > 0) {
            let next = value
                .checked_mul(10)
                .and_then(|v| v.checked_add((bytes[ptr] - b'0') as u32));
            observe(DateParseObservation::Step)?;
            let Some(next) = next else {
                return Ok(None);
            };
            value = next;
            ptr += 1;
            if !scan_to_delim {
                field_len -= 1;
            }
        }
        values[field_idx] = value;
        lengths[field_idx] = ptr - start;
        field_len = 2;
        observe(DateParseObservation::Step)?;

        if ptr == bytes.len() {
            field_idx += 1;
            break;
        }
        if field_idx == 2 && bytes[ptr] == b'T' {
            ptr += 1;
            field_idx += 1;
            observe(DateParseObservation::Step)?;
            continue;
        }
        if field_idx == 5 {
            if bytes[ptr] == b'.' {
                ptr += 1;
                field_len = 6;
                observe(DateParseObservation::Step)?;
            } else if bytes[ptr].is_ascii_digit() {
                field_idx += 1;
                break;
            }
            field_idx += 1;
            continue;
        }
        while ptr < bytes.len()
            && (bytes[ptr].is_ascii_punctuation() || bytes[ptr].is_ascii_whitespace())
        {
            ptr += 1;
            observe(DateParseObservation::Step)?;
        }
        field_idx += 1;
    }

    let parsed_fields = field_idx;
    if parsed_fields < 3 {
        return Ok(None);
    }

    let mut year = values[0] as i32;
    let month = values[1];
    let day = values[2];
    let hour = values[3];
    let minute = values[4];
    let second = values[5];
    let mut microsecond = values[6];

    if lengths[6] > 0 && lengths[6] < 6 {
        let next = microsecond.checked_mul(10u32.pow((6 - lengths[6]) as u32));
        observe(DateParseObservation::Step)?;
        let Some(next) = next else {
            return Ok(None);
        };
        microsecond = next;
    }

    if lengths[0] == 2 {
        year = if year < 70 { year + 2000 } else { year + 1900 };
    }

    let invalid = !(1..=12).contains(&month)
        || day == 0
        || hour > 23
        || minute > 59
        || second > 59
        || microsecond >= 1_000_000;
    observe(DateParseObservation::Step)?;
    if invalid {
        return Ok(None);
    }

    let date = NaiveDate::from_ymd_opt(year, month, day);
    observe(DateParseObservation::Step)?;
    let Some(date) = date else {
        return Ok(None);
    };
    let result = date.and_hms_micro_opt(hour, minute, second, microsecond);
    observe(DateParseObservation::Step)?;
    Ok(result)
}

pub fn parse_datetime(s: &str) -> Option<NaiveDateTime> {
    unobserved(parse_datetime_observed(s, |_| Ok(())))
}

pub(crate) fn parse_datetime_observed<E>(
    s: &str,
    mut observe: impl FnMut(DateParseObservation) -> Result<(), E>,
) -> Result<Option<NaiveDateTime>, E> {
    // Preserve first successful Chrono format, then reject leap seconds before
    // the original flexible fallback. Refusal never tries another parser.
    let mut from_chrono = None;
    for format in [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
    ] {
        observe(DateParseObservation::OpaqueBoundary)?;
        let parsed = NaiveDateTime::parse_from_str(s, format);
        observe(DateParseObservation::OpaqueBoundary)?;
        if let Ok(date) = parsed {
            from_chrono = Some(date);
            break;
        }
    }
    if let Some(date) = from_chrono.filter(|dt| dt.nanosecond() < 1_000_000_000) {
        return Ok(Some(date));
    }
    parse_datetime_flexible(s, &mut observe)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    #[test]
    fn date32_epoch_neighbors_and_extremes_keep_checked_none() {
        assert_eq!(date32_to_naive(0), Some(date(1970, 1, 1)));
        assert_eq!(date32_to_naive(-1), Some(date(1969, 12, 31)));
        assert_eq!(date32_to_naive(1), Some(date(1970, 1, 2)));
        for days in [i32::MIN, i32::MAX, i32::MAX - UNIX_EPOCH_DAY_OFFSET + 1] {
            assert_eq!(date32_to_naive(days), None);
        }
        for boundary in [NaiveDate::MIN, NaiveDate::MAX] {
            let days = boundary.num_days_from_ce() - UNIX_EPOCH_DAY_OFFSET;
            assert_eq!(date32_to_naive(days), Some(boundary));
        }
    }

    #[test]
    fn timestamp_negative_units_use_floor_seconds_and_positive_remainders() {
        let previous = date(1969, 12, 31);
        for (unit, value, nanos) in [
            (TimeUnit::Second, -1, 0),
            (TimeUnit::Millisecond, -1, 999_000_000),
            (TimeUnit::Microsecond, -1, 999_999_000),
            (TimeUnit::Nanosecond, -1, 999_999_999),
        ] {
            assert_eq!(
                timestamp_to_naive(&unit, value),
                previous.and_hms_nano_opt(23, 59, 59, nanos)
            );
            assert_eq!(
                timestamp_to_naive(&unit, 0),
                date(1970, 1, 1).and_hms_opt(0, 0, 0)
            );
        }
        assert_eq!(timestamp_to_naive(&TimeUnit::Second, i64::MAX), None);
        assert_eq!(timestamp_to_naive(&TimeUnit::Second, i64::MIN), None);
        assert_eq!(timestamp_to_naive(&TimeUnit::Millisecond, i64::MAX), None);
        assert_eq!(timestamp_to_naive(&TimeUnit::Millisecond, i64::MIN), None);
    }

    #[test]
    fn timestamp_microsecond_projection_preserves_negative_subseconds() {
        let before = date(1969, 12, 31)
            .and_hms_micro_opt(23, 59, 59, 999_999)
            .unwrap();
        let after = date(1970, 1, 1).and_hms_micro_opt(0, 0, 0, 1).unwrap();
        assert_eq!(naive_to_timestamp_micros(before), -1);
        assert_eq!(naive_to_timestamp_micros(after), 1);
        for dt in [before, after] {
            assert_eq!(
                timestamp_to_naive(&TimeUnit::Microsecond, naive_to_timestamp_micros(dt)),
                Some(dt)
            );
        }
        assert_eq!(
            naive_to_timestamp_micros(
                date(1969, 12, 31)
                    .and_hms_nano_opt(23, 59, 59, 999_999_999)
                    .unwrap()
            ),
            -1
        );
    }

    #[test]
    fn date_parser_keeps_dashed_compact_and_calendar_rejection() {
        assert_eq!(parse_date("2024-02-29"), Some(date(2024, 2, 29)));
        assert_eq!(parse_date("20240229"), Some(date(2024, 2, 29)));
        for invalid in ["", "2023-02-29", "20240230", "2024-13-01", "x2024-01-01"] {
            assert_eq!(parse_date(invalid), None, "{invalid}");
        }
    }

    #[test]
    fn datetime_parser_keeps_flexible_fields_year_pivot_and_fraction_precision() {
        let expected = date(2024, 2, 29).and_hms_opt(12, 34, 56).unwrap();
        for text in [
            "2024-02-29 12:34:56",
            "2024-02-29T12:34:56",
            "20240229123456",
            " 2024/02/29 12:34:56 ",
        ] {
            assert_eq!(parse_datetime(text), Some(expected), "{text}");
        }
        assert_eq!(
            parse_datetime("691231235959"),
            date(2069, 12, 31).and_hms_opt(23, 59, 59)
        );
        assert_eq!(
            parse_datetime("700101000000"),
            date(1970, 1, 1).and_hms_opt(0, 0, 0)
        );
        assert_eq!(
            parse_datetime("2024-02-29T12:34:56.123456789"),
            date(2024, 2, 29).and_hms_nano_opt(12, 34, 56, 123_456_789)
        );
        assert_eq!(
            parse_datetime("20240229123456.12"),
            date(2024, 2, 29).and_hms_micro_opt(12, 34, 56, 120_000)
        );
    }

    #[test]
    fn datetime_parser_rejects_leap_seconds_and_invalid_components() {
        for invalid in [
            "",
            "not a datetime",
            "2023-02-29 12:34:56",
            "2024-01-01 24:00:00",
            "2024-01-01 12:60:00",
            "2016-12-31 23:59:60",
            "2016-12-31T23:59:60.1",
            "20161231235960",
        ] {
            assert_eq!(parse_datetime(invalid), None, "{invalid}");
        }
    }

    #[test]
    fn observed_date_parsers_keep_independent_values_and_real_scan_work() {
        for text in ["2024-02-29", "20240229", "2023-02-29", "nonsense"] {
            let mut events = Vec::new();
            let parsed = parse_date_observed(text, |event| {
                events.push(event);
                Ok::<(), ()>(())
            })
            .unwrap();
            let expected = if text == "2024-02-29" || text == "20240229" {
                Some(date(2024, 2, 29))
            } else {
                None
            };
            assert_eq!(parsed, expected);
            assert_eq!(parsed, parse_date(text));
            assert!(
                events
                    .iter()
                    .all(|event| *event == DateParseObservation::OpaqueBoundary)
            );
        }
        let text = format!("2024-02-29{}12:34:56", ".".repeat(320));
        let mut steps = 0usize;
        let parsed = parse_datetime_observed(&text, |event| {
            if event == DateParseObservation::Step {
                steps += 1;
            }
            Ok::<(), ()>(())
        })
        .unwrap();
        assert_eq!(parsed, date(2024, 2, 29).and_hms_opt(12, 34, 56));
        assert_eq!(parsed, parse_datetime(&text));
        assert!(
            steps >= 320,
            "the original delimiter loop must report its own completed work"
        );
        let invalid = format!("2{}", "T".repeat(320));
        steps = 0;
        assert_eq!(
            parse_datetime_observed(&invalid, |event| {
                if event == DateParseObservation::Step {
                    steps += 1;
                }
                Ok::<(), ()>(())
            })
            .unwrap(),
            None
        );
        assert!(
            steps >= 642,
            "both original prefix and compact-digit scans must complete"
        );
    }

    #[test]
    fn observed_date_parser_refusal_preserves_every_actual_prefix_without_fallback() {
        for text in [
            "2024-02-29 12:34:56",
            "2024-02-29...12:34:56",
            "2016-12-31 23:59:60",
            "nonsense",
        ] {
            let mut events = Vec::new();
            parse_datetime_observed(text, |event| {
                events.push(event);
                Ok::<(), usize>(())
            })
            .unwrap();
            for stop in 0..events.len() {
                let mut observed = Vec::new();
                let result = parse_datetime_observed(text, |event| {
                    assert!(observed.len() <= stop, "callback or fallback after refusal");
                    observed.push(event);
                    if observed.len() == stop + 1 {
                        Err(stop)
                    } else {
                        Ok(())
                    }
                });
                assert_eq!(result, Err(stop));
                assert_eq!(observed, events[..=stop]);
            }
        }
        for text in ["2024-02-29", "20240229", "nonsense"] {
            let mut events = Vec::new();
            parse_date_observed(text, |event| {
                events.push(event);
                Ok::<(), usize>(())
            })
            .unwrap();
            for stop in 0..events.len() {
                let mut observed = Vec::new();
                assert_eq!(
                    parse_date_observed(text, |event| {
                        assert!(observed.len() <= stop);
                        observed.push(event);
                        if observed.len() == stop + 1 {
                            Err(stop)
                        } else {
                            Ok(())
                        }
                    }),
                    Err(stop)
                );
                assert_eq!(observed, events[..=stop]);
            }
        }
    }
}

/// Explicit Trino ARRAY date-cast text profile of this original date author.
/// Unlike the legacy NaiveDate entry it preserves the complete Date32 range.
/// No SQL input limit, new parser module, fallback or global policy is installed.
pub(crate) fn parse_trino_date32_observed<E>(
    text: &str,
    mut observe: impl FnMut(DateParseObservation) -> Result<(), E>,
) -> Result<Option<i32>, E> {
    fn step_char<E>(
        ch: char,
        observe: &mut impl FnMut(DateParseObservation) -> Result<(), E>,
    ) -> Result<(), E> {
        for _ in 0..ch.len_utf8() {
            observe(DateParseObservation::Step)?;
        }
        Ok(())
    }
    // SliceUtf8.trim delegates Character.isWhitespace. This is its Unicode
    // whitespace set, including Java's four information separators and
    // excluding NBSP/FIGURE SPACE/NARROW NBSP and NEL (Rust trim differs).
    fn whitespace(ch: char) -> bool {
        matches!(ch, '\u{0009}'..='\u{000d}' | '\u{001c}'..='\u{0020}' | '\u{1680}'
            | '\u{2000}'..='\u{2006}' | '\u{2008}'..='\u{200a}' | '\u{2028}' | '\u{2029}'
            | '\u{205f}' | '\u{3000}')
    }
    let mut start = 0;
    for ch in text.chars() {
        step_char(ch, &mut observe)?;
        if !whitespace(ch) {
            break;
        }
        start += ch.len_utf8();
    }
    let mut end = text.len();
    for ch in text[start..].chars().rev() {
        step_char(ch, &mut observe)?;
        if !whitespace(ch) {
            break;
        }
        end -= ch.len_utf8();
    }
    let bytes = &text.as_bytes()[start..end];
    // ISODateTimeFormat.date is yearMonthDay. Its NumberFormatter accepts
    // 1..9 ASCII year digits plus optional sign, then unsigned 1..2 month/day
    // digits with the two required '-' literals and no trailing characters.
    fn component<E>(
        bytes: &[u8],
        at: &mut usize,
        max: usize,
        observe: &mut impl FnMut(DateParseObservation) -> Result<(), E>,
    ) -> Result<Option<i64>, E> {
        let begin = *at;
        let mut value = 0_i64;
        while *at < bytes.len() && *at - begin < max && bytes[*at].is_ascii_digit() {
            observe(DateParseObservation::Step)?;
            value = value * 10 + i64::from(bytes[*at] - b'0');
            *at += 1;
        }
        Ok((*at != begin).then_some(value))
    }
    let mut at = 0;
    let negative = bytes.first() == Some(&b'-');
    if matches!(bytes.first(), Some(b'-' | b'+')) {
        observe(DateParseObservation::Step)?;
        at += 1;
    }
    let Some(year) = component(bytes, &mut at, 9, &mut observe)? else {
        return Ok(None);
    };
    if bytes.get(at) != Some(&b'-') {
        return Ok(None);
    }
    observe(DateParseObservation::Step)?;
    at += 1;
    let Some(month) = component(bytes, &mut at, 2, &mut observe)? else {
        return Ok(None);
    };
    if bytes.get(at) != Some(&b'-') {
        return Ok(None);
    }
    observe(DateParseObservation::Step)?;
    at += 1;
    let Some(day) = component(bytes, &mut at, 2, &mut observe)? else {
        return Ok(None);
    };
    if at != bytes.len() {
        return Ok(None);
    }
    let year = if negative { -year } else { year };
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if leap {
                29
            } else {
                28
            }
        }
        _ => return Ok(None),
    };
    if !(1..=days).contains(&day) {
        return Ok(None);
    }
    // ISOChronology is proleptic Gregorian, including year zero. A March-based
    // 400-year era keeps negative-year division exact and avoids Chrono's
    // smaller calendar range. Nine input digits bound every i64 operation.
    observe(DateParseObservation::Step)?;
    // Every accepted i32 day fits Joda's i64 millisecond domain. Values beyond
    // Date32 also fail Trino's final toIntExact and remain conversion Data9.
    Ok(i32::try_from(gregorian_epoch_day_from_valid_parts(year, month, day)).ok())
}

#[cfg(test)]
#[path = "datetime_value_trino_date_tests.rs"]
mod trino_date_profile_tests;

const GREGORIAN_EPOCH_DAY_OFFSET: i64 = 719468;
const DAYS_PER_GREGORIAN_ERA: i64 = 146097;

/// The forward half of the same calendar geometry. Its sole parser caller
/// has already validated month/day and bounds year to nine decimal digits.
fn gregorian_epoch_day_from_valid_parts(year: i64, month: i64, day: i64) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let month = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * DAYS_PER_GREGORIAN_ERA + day_of_era - GREGORIAN_EPOCH_DAY_OFFSET
}

/// Exact proleptic-Gregorian components for every Arrow Date32 value.
/// This is the original date author's inverse geometry, shared with result
/// presentation. Fixed scalar arithmetic needs no source clone or allocation.
/// The complete input range bounds year to -5877641..=5881580, month to 1..=12
/// and day to 1..=31; none of these narrowing casts truncates a valid value.
pub fn date32_to_gregorian(days: i32) -> (i32, u32, u32) {
    let day = i64::from(days) + GREGORIAN_EPOCH_DAY_OFFSET;
    let era = day.div_euclid(DAYS_PER_GREGORIAN_ERA);
    let day_of_era = day - era * DAYS_PER_GREGORIAN_ERA;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36524 - day_of_era / 146096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_from_march = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_from_march + 2) / 5 + 1;
    let month = month_from_march + if month_from_march < 10 { 3 } else { -9 };
    let year = year + i64::from(month <= 2);
    (year as i32, month as u32, day as u32)
}
