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
use super::common::{extract_datetime_array, mysql_format_to_chrono};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{Array, ArrayRef, StringArray};
use chrono::{Datelike, NaiveDateTime};
use std::collections::HashMap;
use std::sync::Arc;

fn format_mysql_datetime(dt: NaiveDateTime, mysql_fmt: &str) -> String {
    let mut out = String::new();
    let mut chars = mysql_fmt.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        let Some(spec) = chars.next() else {
            out.push('%');
            break;
        };
        match spec {
            'Y' => out.push_str(&dt.format("%Y").to_string()),
            'y' => out.push_str(&dt.format("%y").to_string()),
            'm' | 'c' => out.push_str(&dt.format("%m").to_string()),
            'd' | 'e' => out.push_str(&dt.format("%d").to_string()),
            'H' => out.push_str(&dt.format("%H").to_string()),
            'h' | 'I' => out.push_str(&dt.format("%I").to_string()),
            'i' => out.push_str(&dt.format("%M").to_string()),
            's' | 'S' => out.push_str(&dt.format("%S").to_string()),
            // MySQL `%f` is always microseconds (6 digits).
            'f' => out.push_str(&format!("{:06}", dt.and_utc().timestamp_subsec_micros())),
            'T' => out.push_str(&dt.format("%H:%M:%S").to_string()),
            other => {
                out.push('%');
                out.push(other);
            }
        }
    }
    out
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JodaFormatToken {
    Year4,
    Year2,
    Month2,
    Day2,
    Hour24,
    Hour12,
    Minute2,
    Second2,
    WeekOfYear,
    Literal(u8),
}

#[inline]
fn invalid_joda_format_error(fmt: &str) -> String {
    format!("The format parameter {} is invalid format", fmt)
}

fn compile_joda_format(fmt: &str) -> Result<Vec<JodaFormatToken>, String> {
    let bytes = fmt.as_bytes();
    let mut i = 0usize;
    let mut out = Vec::with_capacity(bytes.len());
    while i < bytes.len() {
        let rest = &bytes[i..];
        if rest.starts_with(b"yyyy") {
            out.push(JodaFormatToken::Year4);
            i += 4;
            continue;
        }
        if rest.starts_with(b"yy") {
            out.push(JodaFormatToken::Year2);
            i += 2;
            continue;
        }
        if rest.starts_with(b"ww") {
            out.push(JodaFormatToken::WeekOfYear);
            i += 2;
            continue;
        }
        if rest.starts_with(b"MM") {
            out.push(JodaFormatToken::Month2);
            i += 2;
            continue;
        }
        if rest.starts_with(b"dd") {
            out.push(JodaFormatToken::Day2);
            i += 2;
            continue;
        }
        if rest.starts_with(b"HH") {
            out.push(JodaFormatToken::Hour24);
            i += 2;
            continue;
        }
        if rest.starts_with(b"hh") {
            out.push(JodaFormatToken::Hour12);
            i += 2;
            continue;
        }
        if rest.starts_with(b"mm") {
            out.push(JodaFormatToken::Minute2);
            i += 2;
            continue;
        }
        if rest.starts_with(b"ss") {
            out.push(JodaFormatToken::Second2);
            i += 2;
            continue;
        }

        let b = bytes[i];
        if b.is_ascii_alphabetic() {
            return Err(invalid_joda_format_error(fmt));
        }
        out.push(JodaFormatToken::Literal(b));
        i += 1;
    }

    Ok(out)
}

fn format_joda_datetime(dt: NaiveDateTime, tokens: &[JodaFormatToken]) -> String {
    let mut out = String::with_capacity(tokens.len() * 2);
    for token in tokens {
        match token {
            JodaFormatToken::Year4 => out.push_str(&format!("{:04}", dt.year())),
            JodaFormatToken::Year2 => out.push_str(&format!("{:02}", dt.year().rem_euclid(100))),
            JodaFormatToken::Month2 => out.push_str(&format!("{:02}", dt.month())),
            JodaFormatToken::Day2 => out.push_str(&format!("{:02}", dt.day())),
            JodaFormatToken::Hour24 => out.push_str(&dt.format("%H").to_string()),
            JodaFormatToken::Hour12 => out.push_str(&dt.format("%I").to_string()),
            JodaFormatToken::Minute2 => out.push_str(&dt.format("%M").to_string()),
            JodaFormatToken::Second2 => out.push_str(&dt.format("%S").to_string()),
            JodaFormatToken::WeekOfYear => out.push_str(&format!("{:02}", dt.iso_week().week())),
            JodaFormatToken::Literal(b) => out.push(char::from(*b)),
        }
    }
    out
}

#[inline]
fn eval_date_format_inner(
    arena: &ExprArena,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let date_arr = arena.eval(args[0], chunk)?;
    let fmt_arr = arena.eval(args[1], chunk)?;
    let fmt_arr = fmt_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "date_format expects string format".to_string())?;
    let dts = extract_datetime_array(&date_arr)?;
    let mut out = Vec::with_capacity(dts.len());
    for (i, dt) in dts.into_iter().enumerate() {
        if fmt_arr.is_null(i) {
            out.push(None);
            continue;
        }
        let fmt = mysql_format_to_chrono(fmt_arr.value(i)).replace("%f", "%6f");
        let v = dt.and_then(|d| {
            let s = d.format(&fmt).to_string();
            (s.len() <= 128).then_some(s)
        });
        out.push(v);
    }
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}

pub fn eval_date_format(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_date_format_inner(arena, args, chunk)
}

pub fn eval_strftime(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_date_format_inner(arena, args, chunk)
}

pub fn eval_jodatime_format(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let date_arr = arena.eval(args[0], chunk)?;
    let fmt_arr = arena.eval(args[1], chunk)?;
    let fmt_arr = fmt_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "jodatime_format expects string format".to_string())?;
    let dts = extract_datetime_array(&date_arr)?;
    let mut fmt_cache: HashMap<String, Vec<JodaFormatToken>> = HashMap::new();
    let mut out = Vec::with_capacity(dts.len());
    for (i, dt) in dts.into_iter().enumerate() {
        if fmt_arr.is_null(i) {
            out.push(None);
            continue;
        }
        let fmt = fmt_arr.value(i);
        if !fmt_cache.contains_key(fmt) {
            fmt_cache.insert(fmt.to_string(), compile_joda_format(fmt)?);
        }
        let tokens = fmt_cache
            .get(fmt)
            .expect("format cache must contain validated format");
        let v = dt.map(|d| format_joda_datetime(d, tokens));
        out.push(v);
    }
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use crate::exec::expr::function::date::test_utils::assert_date_function_logic;

    #[test]
    fn test_date_format_logic() {
        assert_date_function_logic("date_format");
    }

    #[test]
    fn test_strftime_logic() {
        assert_date_function_logic("strftime");
    }
}
