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
    mysql_format_to_chrono, naive_to_date32, parse_date, parse_datetime_with_pattern,
    to_timestamp_value,
};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{Array, ArrayRef, Date32Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::DataType;
use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, Weekday};
use std::sync::Arc;

fn parse_weekday_name(name: &str) -> Option<Weekday> {
    match name.to_ascii_lowercase().as_str() {
        "sunday" => Some(Weekday::Sun),
        "monday" => Some(Weekday::Mon),
        "tuesday" => Some(Weekday::Tue),
        "wednesday" => Some(Weekday::Wed),
        "thursday" => Some(Weekday::Thu),
        "friday" => Some(Weekday::Fri),
        "saturday" => Some(Weekday::Sat),
        _ => None,
    }
}

fn parse_mysql_weekyear_sunday(input: &str) -> Option<NaiveDateTime> {
    let mut parts = input.split_whitespace();
    let year_week = parts.next()?;
    let weekday_name = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    if year_week.len() != 6 || !year_week.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }

    let year = year_week[0..4].parse::<i32>().ok()?;
    let week = year_week[4..6].parse::<u32>().ok()?;
    if !(1..=53).contains(&week) {
        return None;
    }

    let weekday = parse_weekday_name(weekday_name)?;
    let jan1 = NaiveDate::from_ymd_opt(year, 1, 1)?;
    let days_to_first_sunday = (7 - jan1.weekday().num_days_from_sunday()) % 7;
    let first_sunday = jan1 + Duration::days(days_to_first_sunday as i64);
    let date = first_sunday
        + Duration::days((week as i64 - 1) * 7 + weekday.num_days_from_sunday() as i64);
    date.and_hms_opt(0, 0, 0)
}

#[inline]
fn eval_str_to_date_inner(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let str_arr = arena.eval(args[0], chunk)?;
    let fmt_arr = arena.eval(args[1], chunk)?;
    let s_arr = str_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "str_to_date expects string".to_string())?;
    let f_arr = fmt_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "str_to_date expects string".to_string())?;
    let output_type = arena.data_type(expr).cloned().unwrap_or(DataType::Date32);
    let mut out = Vec::with_capacity(s_arr.len());
    for i in 0..s_arr.len() {
        if s_arr.is_null(i) || f_arr.is_null(i) {
            out.push(None);
            continue;
        }
        let raw_fmt = f_arr.value(i);
        let dt = if raw_fmt.eq_ignore_ascii_case("%X%V %W") {
            parse_mysql_weekyear_sunday(s_arr.value(i))
        } else {
            let fmt = mysql_format_to_chrono(raw_fmt);
            parse_datetime_with_pattern(s_arr.value(i), &fmt)
                .or_else(|| {
                    NaiveDate::parse_from_str(s_arr.value(i), &fmt)
                        .ok()
                        .and_then(|d| d.and_hms_opt(0, 0, 0))
                })
                .or_else(|| parse_date(s_arr.value(i)).map(|d| d.and_hms_opt(0, 0, 0).unwrap()))
        };
        let v = match output_type {
            DataType::Date32 => dt.map(|d| naive_to_date32(d.date()) as i64),
            _ => dt.and_then(|d| to_timestamp_value(d, &output_type).ok()),
        };
        out.push(v);
    }
    if matches!(output_type, DataType::Date32) {
        let out: Vec<Option<i32>> = out.into_iter().map(|v| v.map(|x| x as i32)).collect();
        Ok(Arc::new(Date32Array::from(out)) as ArrayRef)
    } else {
        Ok(Arc::new(TimestampMicrosecondArray::from(out)) as ArrayRef)
    }
}

pub fn eval_str_to_date(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_str_to_date_inner(arena, expr, args, chunk)
}

pub fn eval_str2date(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_str_to_date_inner(arena, expr, args, chunk)
}

#[cfg(test)]
mod tests {
    use crate::exec::expr::function::date::test_utils::assert_date_function_logic;

    #[test]
    fn test_str_to_date_logic() {
        assert_date_function_logic("str_to_date");
    }

    #[test]
    fn test_str2date_logic() {
        assert_date_function_logic("str2date");
    }
}
