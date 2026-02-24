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
use super::common::{extract_datetime_array, extract_i64_array, to_timestamp_value};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{ArrayRef, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime};
use std::sync::Arc;

fn add_months_to_date(date: NaiveDate, months: i32) -> NaiveDate {
    let mut year = date.year();
    let mut month = date.month() as i32 - 1 + months;
    year += month.div_euclid(12);
    month = month.rem_euclid(12) + 1;
    let last_day = last_day_of_month(year, month as u32);
    let day = date.day().min(last_day);
    NaiveDate::from_ymd_opt(year, month as u32, day).unwrap()
}

pub(super) fn add_months_to_datetime(dt: NaiveDateTime, months: i32) -> NaiveDateTime {
    let date = add_months_to_date(dt.date(), months);
    date.and_time(dt.time())
}

fn last_day_of_month(year: i32, month: u32) -> u32 {
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    let first_next = NaiveDate::from_ymd_opt(next_year, next_month, 1).unwrap();
    (first_next - Duration::days(1)).day()
}

#[inline]
fn eval_with_factor(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    factor: i32,
) -> Result<ArrayRef, String> {
    let date_arr = arena.eval(args[0], chunk)?;
    let month_arr = arena.eval(args[1], chunk)?;
    let dts = extract_datetime_array(&date_arr)?;
    let months = extract_i64_array(&month_arr, "add_months")?;
    let output_type = arena
        .data_type(expr)
        .cloned()
        .unwrap_or(DataType::Timestamp(TimeUnit::Microsecond, None));
    let len = dts.len().max(months.len());
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        let dt = if dts.len() == 1 { dts[0] } else { dts[i] };
        let month = if months.len() == 1 {
            months[0]
        } else {
            months[i]
        };
        let Some(month) = month else {
            out.push(None);
            continue;
        };
        let m = month as i32 * factor;
        let v = dt.map(|datetime| add_months_to_datetime(datetime, m));
        let v = v.and_then(|d| to_timestamp_value(d, &output_type).ok());
        out.push(v);
    }
    Ok(Arc::new(TimestampMicrosecondArray::from(out)) as ArrayRef)
}

pub fn eval_add_months(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_with_factor(arena, expr, args, chunk, 1)
}

pub fn eval_months_add(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_with_factor(arena, expr, args, chunk, 1)
}

pub fn eval_months_sub(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_with_factor(arena, expr, args, chunk, -1)
}

pub fn eval_quarters_add(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_with_factor(arena, expr, args, chunk, 3)
}

pub fn eval_quarters_sub(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_with_factor(arena, expr, args, chunk, -3)
}

pub fn eval_years_add(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_with_factor(arena, expr, args, chunk, 12)
}

pub fn eval_years_sub(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_with_factor(arena, expr, args, chunk, -12)
}

#[cfg(test)]
mod tests {
    use crate::exec::expr::function::date::test_utils::assert_date_function_logic;

    #[test]
    fn test_add_months_logic() {
        assert_date_function_logic("add_months");
    }

    #[test]
    fn test_months_add_logic() {
        assert_date_function_logic("months_add");
    }

    #[test]
    fn test_months_sub_logic() {
        assert_date_function_logic("months_sub");
    }

    #[test]
    fn test_quarters_add_logic() {
        assert_date_function_logic("quarters_add");
    }

    #[test]
    fn test_quarters_sub_logic() {
        assert_date_function_logic("quarters_sub");
    }

    #[test]
    fn test_years_add_logic() {
        assert_date_function_logic("years_add");
    }

    #[test]
    fn test_years_sub_logic() {
        assert_date_function_logic("years_sub");
    }
}
