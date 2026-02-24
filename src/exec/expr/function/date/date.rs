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
use super::common::{
    extract_date_array, extract_datetime_array, extract_i64_array, naive_to_date32,
    to_timestamp_value,
};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{Array, ArrayRef, Date32Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{DateTime, Local, NaiveDate, NaiveDateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;

#[inline]
fn eval_to_date_inner(
    arena: &ExprArena,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let arr = arena.eval(args[0], chunk)?;
    let dates = extract_date_array(&arr)?;
    let out: Vec<Option<i32>> = dates.into_iter().map(|d| d.map(naive_to_date32)).collect();
    Ok(Arc::new(Date32Array::from(out)) as ArrayRef)
}

#[inline]
fn eval_to_datetime_inner(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    timezone_aware: bool,
) -> Result<ArrayRef, String> {
    let output_type = arena
        .data_type(expr)
        .cloned()
        .unwrap_or(DataType::Timestamp(TimeUnit::Microsecond, None));

    if args.len() == 1 {
        let arr = arena.eval(args[0], chunk)?;
        if let Ok(dts) = extract_datetime_array(&arr) {
            let mut out = Vec::with_capacity(dts.len());
            for dt in dts {
                let v = dt.and_then(|d| to_timestamp_value(d, &output_type).ok());
                out.push(v);
            }
            return Ok(Arc::new(TimestampMicrosecondArray::from(out)) as ArrayRef);
        }

        let values = extract_i64_array(&arr, "to_datetime")?;
        let mut out = Vec::with_capacity(values.len());
        for value in values {
            let Some(v) = value else {
                out.push(None);
                continue;
            };
            let Some((secs, micros)) = split_epoch_value(v, 0) else {
                out.push(None);
                continue;
            };
            let Some(dt) = epoch_to_datetime(secs, micros, timezone_aware) else {
                out.push(None);
                continue;
            };
            out.push(to_timestamp_value(dt, &output_type).ok());
        }
        return Ok(Arc::new(TimestampMicrosecondArray::from(out)) as ArrayRef);
    }

    let value_arr = arena.eval(args[0], chunk)?;
    let scale_arr = arena.eval(args[1], chunk)?;
    let values = extract_i64_array(&value_arr, "to_datetime")?;
    let scales = extract_i64_array(&scale_arr, "to_datetime")?;
    let mut out = Vec::with_capacity(values.len());

    for i in 0..values.len() {
        let Some(v) = values[i] else {
            out.push(None);
            continue;
        };
        let Some(scale) = scales[i] else {
            out.push(None);
            continue;
        };

        let Some((secs, micros)) = split_epoch_value(v, scale) else {
            out.push(None);
            continue;
        };
        let Some(dt) = epoch_to_datetime(secs, micros, timezone_aware) else {
            out.push(None);
            continue;
        };

        out.push(to_timestamp_value(dt, &output_type).ok());
    }
    Ok(Arc::new(TimestampMicrosecondArray::from(out)) as ArrayRef)
}

fn split_epoch_value(value: i64, scale: i64) -> Option<(i64, u32)> {
    match scale {
        0 => Some((value, 0)),
        3 => {
            let secs = value.div_euclid(1_000);
            let micros = (value.rem_euclid(1_000) as u32) * 1_000;
            Some((secs, micros))
        }
        6 => {
            let secs = value.div_euclid(1_000_000);
            let micros = value.rem_euclid(1_000_000) as u32;
            Some((secs, micros))
        }
        _ => None,
    }
}

fn epoch_to_datetime(seconds: i64, micros: u32, timezone_aware: bool) -> Option<NaiveDateTime> {
    let dt_utc = DateTime::<Utc>::from_timestamp(seconds, micros * 1_000)?;
    if timezone_aware {
        Some(dt_utc.with_timezone(&Local).naive_local())
    } else {
        Some(dt_utc.naive_utc())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TeraToken {
    Year4,
    Year2,
    Month,
    Day,
    Hour12,
    Hour24,
    Minute,
    Second,
    Am,
    Pm,
    Literal(u8),
}

#[inline]
fn invalid_tera_format_error(fmt: &str) -> String {
    format!("The format parameter {} is invalid format", fmt)
}

fn compile_tera_format(fmt: &str) -> Result<Vec<TeraToken>, String> {
    let bytes = fmt.as_bytes();
    let mut i = 0usize;
    let mut out = Vec::with_capacity(bytes.len());
    while i < bytes.len() {
        let rest = &bytes[i..];
        if rest.starts_with(b"yyyy") {
            out.push(TeraToken::Year4);
            i += 4;
            continue;
        }
        if rest.starts_with(b"hh24") {
            out.push(TeraToken::Hour24);
            i += 4;
            continue;
        }
        if rest.starts_with(b"yy") {
            out.push(TeraToken::Year2);
            i += 2;
            continue;
        }
        if rest.starts_with(b"mm") {
            out.push(TeraToken::Month);
            i += 2;
            continue;
        }
        if rest.starts_with(b"dd") {
            out.push(TeraToken::Day);
            i += 2;
            continue;
        }
        if rest.starts_with(b"hh") {
            out.push(TeraToken::Hour12);
            i += 2;
            continue;
        }
        if rest.starts_with(b"mi") {
            out.push(TeraToken::Minute);
            i += 2;
            continue;
        }
        if rest.starts_with(b"ss") {
            out.push(TeraToken::Second);
            i += 2;
            continue;
        }
        if rest.starts_with(b"am") {
            out.push(TeraToken::Am);
            i += 2;
            continue;
        }
        if rest.starts_with(b"pm") {
            out.push(TeraToken::Pm);
            i += 2;
            continue;
        }

        let b = bytes[i];
        if b.is_ascii_alphabetic() {
            return Err(invalid_tera_format_error(fmt));
        }
        out.push(TeraToken::Literal(b));
        i += 1;
    }

    if !out
        .iter()
        .any(|t| matches!(t, TeraToken::Year4 | TeraToken::Year2))
    {
        return Err(invalid_tera_format_error(fmt));
    }
    Ok(out)
}

fn compile_joda_format(fmt: &str) -> Result<Vec<TeraToken>, String> {
    let bytes = fmt.as_bytes();
    let mut i = 0usize;
    let mut out = Vec::with_capacity(bytes.len());
    while i < bytes.len() {
        let rest = &bytes[i..];
        if rest.starts_with(b"yyyy") {
            out.push(TeraToken::Year4);
            i += 4;
            continue;
        }
        if rest.starts_with(b"HH") {
            out.push(TeraToken::Hour24);
            i += 2;
            continue;
        }
        if rest.starts_with(b"yy") {
            out.push(TeraToken::Year2);
            i += 2;
            continue;
        }
        if rest.starts_with(b"MM") {
            out.push(TeraToken::Month);
            i += 2;
            continue;
        }
        if rest.starts_with(b"dd") {
            out.push(TeraToken::Day);
            i += 2;
            continue;
        }
        if rest.starts_with(b"hh") {
            out.push(TeraToken::Hour12);
            i += 2;
            continue;
        }
        if rest.starts_with(b"mm") {
            out.push(TeraToken::Minute);
            i += 2;
            continue;
        }
        if rest.starts_with(b"ss") {
            out.push(TeraToken::Second);
            i += 2;
            continue;
        }

        let b = bytes[i];
        if b.is_ascii_alphabetic() {
            return Err(invalid_tera_format_error(fmt));
        }
        out.push(TeraToken::Literal(b));
        i += 1;
    }

    if !out
        .iter()
        .any(|t| matches!(t, TeraToken::Year4 | TeraToken::Year2))
    {
        return Err(invalid_tera_format_error(fmt));
    }
    Ok(out)
}

#[inline]
fn parse_fixed_digits(input: &[u8], pos: &mut usize, width: usize) -> Option<u32> {
    if *pos + width > input.len() {
        return None;
    }
    let mut value: u32 = 0;
    for _ in 0..width {
        let b = input[*pos];
        if !b.is_ascii_digit() {
            return None;
        }
        value = value.checked_mul(10)?;
        value = value.checked_add(u32::from(b - b'0'))?;
        *pos += 1;
    }
    Some(value)
}

#[inline]
fn token_is_numeric(token: TeraToken) -> bool {
    matches!(
        token,
        TeraToken::Year4
            | TeraToken::Year2
            | TeraToken::Month
            | TeraToken::Day
            | TeraToken::Hour12
            | TeraToken::Hour24
            | TeraToken::Minute
            | TeraToken::Second
    )
}

#[inline]
fn is_date_only_format(tokens: &[TeraToken]) -> bool {
    !tokens.iter().any(|t| {
        matches!(
            t,
            TeraToken::Hour12
                | TeraToken::Hour24
                | TeraToken::Minute
                | TeraToken::Second
                | TeraToken::Am
                | TeraToken::Pm
        )
    })
}

fn parse_one_or_two_digits(input: &[u8], pos: &mut usize, force_two_digits: bool) -> Option<u32> {
    if force_two_digits {
        return parse_fixed_digits(input, pos, 2);
    }
    if *pos >= input.len() || !input[*pos].is_ascii_digit() {
        return None;
    }
    let mut value = u32::from(input[*pos] - b'0');
    *pos += 1;
    if *pos < input.len() && input[*pos].is_ascii_digit() {
        value = value.checked_mul(10)?;
        value = value.checked_add(u32::from(input[*pos] - b'0'))?;
        *pos += 1;
    }
    Some(value)
}

fn parse_am_pm(input: &[u8], pos: &mut usize, expect_pm: bool) -> Option<bool> {
    if *pos + 2 > input.len() {
        return None;
    }
    let a = input[*pos].to_ascii_lowercase();
    let b = input[*pos + 1].to_ascii_lowercase();
    *pos += 2;
    match (a, b) {
        (b'a', b'm') if !expect_pm => Some(false),
        (b'p', b'm') if expect_pm => Some(true),
        _ => None,
    }
}

fn parse_tera_datetime(input: &str, tokens: &[TeraToken]) -> Option<NaiveDateTime> {
    let bytes = input.as_bytes();
    let mut pos = 0usize;

    let mut year: Option<i32> = None;
    let mut month: u32 = 1;
    let mut day: u32 = 1;
    let mut hour12: Option<u32> = None;
    let mut hour24: Option<u32> = None;
    let mut minute: u32 = 0;
    let mut second: u32 = 0;
    let mut meridian: Option<bool> = None;

    for (idx, token) in tokens.iter().copied().enumerate() {
        let next_is_numeric = tokens.get(idx + 1).copied().is_some_and(token_is_numeric);
        match token {
            TeraToken::Year4 => {
                year = Some(parse_fixed_digits(bytes, &mut pos, 4)? as i32);
            }
            TeraToken::Year2 => {
                // Match StarRocks loosely by mapping 2-digit year into 2000-based range.
                year = Some(2000 + parse_fixed_digits(bytes, &mut pos, 2)? as i32);
            }
            TeraToken::Month => {
                month = parse_one_or_two_digits(bytes, &mut pos, next_is_numeric)?;
            }
            TeraToken::Day => {
                day = parse_one_or_two_digits(bytes, &mut pos, next_is_numeric)?;
            }
            TeraToken::Hour12 => {
                hour12 = Some(parse_one_or_two_digits(bytes, &mut pos, next_is_numeric)?);
            }
            TeraToken::Hour24 => {
                hour24 = Some(parse_one_or_two_digits(bytes, &mut pos, next_is_numeric)?);
            }
            TeraToken::Minute => {
                minute = parse_one_or_two_digits(bytes, &mut pos, next_is_numeric)?;
            }
            TeraToken::Second => {
                second = parse_one_or_two_digits(bytes, &mut pos, next_is_numeric)?;
            }
            TeraToken::Am => {
                meridian = parse_am_pm(bytes, &mut pos, false);
                meridian?;
            }
            TeraToken::Pm => {
                meridian = parse_am_pm(bytes, &mut pos, true);
                meridian?;
            }
            TeraToken::Literal(b) => {
                if bytes.get(pos).copied() != Some(b) {
                    return None;
                }
                pos += 1;
            }
        }
    }

    if pos != bytes.len() {
        return None;
    }

    let hour = if let Some(h24) = hour24 {
        if h24 > 23 || meridian.is_some() || hour12.is_some() {
            return None;
        }
        h24
    } else if let Some(h12) = hour12 {
        if let Some(pm) = meridian {
            if h12 == 0 || h12 > 12 {
                return None;
            }
            if pm {
                if h12 == 12 { 12 } else { h12 + 12 }
            } else if h12 == 12 {
                0
            } else {
                h12
            }
        } else {
            if h12 > 23 {
                return None;
            }
            h12
        }
    } else {
        if meridian.is_some() {
            return None;
        }
        0
    };

    if minute > 59 || second > 59 {
        return None;
    }
    let y = year?;
    let d = NaiveDate::from_ymd_opt(y, month, day)?;
    d.and_hms_opt(hour, minute, second)
}

pub fn eval_date(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_to_date_inner(arena, args, chunk)
}

pub fn eval_to_date(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_to_date_inner(arena, args, chunk)
}

pub fn eval_to_tera_date(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() == 1 {
        return eval_to_date_inner(arena, args, chunk);
    }

    let value_arr = arena.eval(args[0], chunk)?;
    let fmt_arr = arena.eval(args[1], chunk)?;
    let fmt_arr = fmt_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "to_tera_date expects string format".to_string())?;
    let mut fmt_cache: HashMap<String, Vec<TeraToken>> = HashMap::new();

    let out = if let Some(values) = value_arr.as_any().downcast_ref::<StringArray>() {
        let mut out = Vec::with_capacity(values.len());
        for i in 0..values.len() {
            if fmt_arr.is_null(i) || values.is_null(i) {
                out.push(None);
                continue;
            }
            let fmt = fmt_arr.value(i);
            if !fmt_cache.contains_key(fmt) {
                fmt_cache.insert(fmt.to_string(), compile_tera_format(fmt)?);
            }
            let tokens = fmt_cache
                .get(fmt)
                .expect("format cache must contain validated format");
            let mut parsed = parse_tera_datetime(values.value(i), tokens);
            if parsed.is_none() && is_date_only_format(tokens) {
                let raw = values.value(i);
                if let Some(cut) = raw.find([' ', 'T']) {
                    parsed = parse_tera_datetime(&raw[..cut], tokens);
                }
            }
            out.push(parsed.map(|dt| naive_to_date32(dt.date())));
        }
        out
    } else {
        let dates = extract_date_array(&value_arr)?;
        let mut out = Vec::with_capacity(dates.len());
        for i in 0..dates.len() {
            if fmt_arr.is_null(i) {
                out.push(None);
                continue;
            }
            let fmt = fmt_arr.value(i);
            if !fmt_cache.contains_key(fmt) {
                fmt_cache.insert(fmt.to_string(), compile_tera_format(fmt)?);
            }
            out.push(dates[i].map(naive_to_date32));
        }
        out
    };
    Ok(Arc::new(Date32Array::from(out)) as ArrayRef)
}

pub fn eval_to_tera_timestamp(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_timestamp_with_custom_format(
        arena,
        expr,
        args,
        chunk,
        "to_tera_timestamp expects string format",
        compile_tera_format,
    )
}

pub fn eval_str_to_jodatime(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_timestamp_with_custom_format(
        arena,
        expr,
        args,
        chunk,
        "str_to_jodatime expects string format",
        compile_joda_format,
    )
}

fn eval_timestamp_with_custom_format(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    fmt_error: &'static str,
    compile_fn: fn(&str) -> Result<Vec<TeraToken>, String>,
) -> Result<ArrayRef, String> {
    if args.len() == 1 {
        return eval_to_datetime_inner(arena, expr, args, chunk, true);
    }

    let value_arr = arena.eval(args[0], chunk)?;
    let fmt_arr = arena.eval(args[1], chunk)?;
    let fmt_arr = fmt_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| fmt_error.to_string())?;
    let output_type = arena
        .data_type(expr)
        .cloned()
        .unwrap_or(DataType::Timestamp(TimeUnit::Microsecond, None));
    let mut fmt_cache: HashMap<String, Vec<TeraToken>> = HashMap::new();

    let out = if let Some(values) = value_arr.as_any().downcast_ref::<StringArray>() {
        let mut out = Vec::with_capacity(values.len());
        for i in 0..values.len() {
            if fmt_arr.is_null(i) || values.is_null(i) {
                out.push(None);
                continue;
            }
            let fmt = fmt_arr.value(i);
            if !fmt_cache.contains_key(fmt) {
                fmt_cache.insert(fmt.to_string(), compile_fn(fmt)?);
            }
            let parsed = parse_tera_datetime(
                values.value(i),
                fmt_cache
                    .get(fmt)
                    .expect("format cache must contain validated format"),
            );
            let ts = parsed.and_then(|dt| to_timestamp_value(dt, &output_type).ok());
            out.push(ts);
        }
        out
    } else {
        let dts = extract_datetime_array(&value_arr)?;
        let mut out = Vec::with_capacity(dts.len());
        for i in 0..dts.len() {
            if fmt_arr.is_null(i) {
                out.push(None);
                continue;
            }
            let fmt = fmt_arr.value(i);
            if !fmt_cache.contains_key(fmt) {
                fmt_cache.insert(fmt.to_string(), compile_fn(fmt)?);
            }
            let ts = dts[i].and_then(|dt| to_timestamp_value(dt, &output_type).ok());
            out.push(ts);
        }
        out
    };
    Ok(Arc::new(TimestampMicrosecondArray::from(out)) as ArrayRef)
}

pub fn eval_to_datetime(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_to_datetime_inner(arena, expr, args, chunk, true)
}

pub fn eval_to_datetime_ntz(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_to_datetime_inner(arena, expr, args, chunk, false)
}

pub fn eval_timestamp(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_to_datetime_inner(arena, expr, args, chunk, true)
}

#[cfg(test)]
mod tests {
    use super::{compile_tera_format, parse_tera_datetime};
    use crate::exec::expr::function::date::test_utils::assert_date_function_logic;
    use chrono::NaiveDate;

    #[test]
    fn test_date_logic() {
        assert_date_function_logic("date");
    }

    #[test]
    fn test_to_date_logic() {
        assert_date_function_logic("to_date");
    }

    #[test]
    fn test_to_tera_date_logic() {
        assert_date_function_logic("to_tera_date");
    }

    #[test]
    fn test_to_datetime_logic() {
        assert_date_function_logic("to_datetime");
    }

    #[test]
    fn test_timestamp_logic() {
        assert_date_function_logic("timestamp");
    }

    #[test]
    fn test_tera_datetime_parser_basic_patterns() {
        let fmt = compile_tera_format("yyyy/mm/dd hh24:mi:ss").expect("compile format");
        let dt = parse_tera_datetime("1988/04/08 2:3:4", &fmt).expect("parse datetime");
        assert_eq!(
            dt,
            NaiveDate::from_ymd_opt(1988, 4, 8)
                .expect("valid date")
                .and_hms_opt(2, 3, 4)
                .expect("valid time")
        );

        let fmt_year = compile_tera_format("yyyy").expect("compile year format");
        let dt_year = parse_tera_datetime("1988", &fmt_year).expect("parse year-only");
        assert_eq!(
            dt_year,
            NaiveDate::from_ymd_opt(1988, 1, 1)
                .expect("valid date")
                .and_hms_opt(0, 0, 0)
                .expect("valid time")
        );
    }

    #[test]
    fn test_tera_datetime_parser_am_pm() {
        let fmt = compile_tera_format("yyyy/mm/dd hh pm:mi:ss").expect("compile format");
        let dt = parse_tera_datetime("1988/04/08 02 pm:3:4", &fmt).expect("parse pm datetime");
        assert_eq!(
            dt,
            NaiveDate::from_ymd_opt(1988, 4, 8)
                .expect("valid date")
                .and_hms_opt(14, 3, 4)
                .expect("valid time")
        );

        let fmt_am = compile_tera_format("yyyy/mm/dd hh am:mi:ss").expect("compile format");
        assert!(parse_tera_datetime("1988/04/08 02 pm:3:4", &fmt_am).is_none());
    }

    #[test]
    fn test_tera_datetime_format_validation() {
        let err = compile_tera_format(";YYYYmm:dd").expect_err("format should be invalid");
        assert_eq!(err, "The format parameter ;YYYYmm:dd is invalid format");
    }
}
