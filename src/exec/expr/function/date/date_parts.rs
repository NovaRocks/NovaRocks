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
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use chrono::{Datelike, Timelike};
use std::sync::Arc;

enum DatePart {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    DayOfWeek,
    DayOfWeekIso,
    WeekDay,
    DayOfYear,
    Week,
    Quarter,
    DayName,
    MonthName,
}

#[inline]
fn eval_part_int(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    part: DatePart,
) -> Result<ArrayRef, String> {
    let arr = arena.eval(args[0], chunk)?;
    let dts = super::common::extract_datetime_array(&arr)?;
    let mut out = Vec::with_capacity(dts.len());
    for dt in dts {
        let v = dt.map(|d| match part {
            DatePart::Year => d.year() as i64,
            DatePart::Month => d.month() as i64,
            DatePart::Day => d.day() as i64,
            DatePart::Hour => d.hour() as i64,
            DatePart::Minute => d.minute() as i64,
            DatePart::Second => d.second() as i64,
            DatePart::DayOfWeek => d.weekday().number_from_sunday() as i64,
            DatePart::DayOfWeekIso => d.weekday().number_from_monday() as i64,
            DatePart::WeekDay => d.weekday().num_days_from_monday() as i64,
            DatePart::DayOfYear => d.ordinal() as i64,
            DatePart::Week => d.iso_week().week() as i64,
            DatePart::Quarter => ((d.month() - 1) / 3 + 1) as i64,
            _ => 0,
        });
        out.push(v);
    }
    let out = Arc::new(Int64Array::from(out)) as ArrayRef;
    let expected_type = arena.data_type(expr).cloned().unwrap_or(DataType::Int64);
    if out.data_type() == &expected_type {
        return Ok(out);
    }
    cast(&out, &expected_type).map_err(|e| {
        format!(
            "date part output cast failed from {:?} to {:?}: {}",
            out.data_type(),
            expected_type,
            e
        )
    })
}

#[inline]
fn eval_part_string(
    arena: &ExprArena,
    args: &[ExprId],
    chunk: &Chunk,
    part: DatePart,
) -> Result<ArrayRef, String> {
    let arr = arena.eval(args[0], chunk)?;
    let dts = super::common::extract_datetime_array(&arr)?;
    let mut out = Vec::with_capacity(dts.len());
    for dt in dts {
        let v = dt.map(|d| match part {
            DatePart::DayName => d.format("%A").to_string(),
            DatePart::MonthName => d.format("%B").to_string(),
            _ => "".to_string(),
        });
        out.push(v);
    }
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}

pub fn eval_day(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_part_int(arena, expr, args, chunk, DatePart::Day)
}

pub fn eval_dayofmonth(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_part_int(arena, expr, args, chunk, DatePart::Day)
}

pub fn eval_dayofweek(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_part_int(arena, expr, args, chunk, DatePart::DayOfWeek)
}

pub fn eval_dayofyear(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_part_int(arena, expr, args, chunk, DatePart::DayOfYear)
}

pub fn eval_dayofweek_iso(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_part_int(arena, expr, args, chunk, DatePart::DayOfWeekIso)
}

pub fn eval_weekday(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_part_int(arena, expr, args, chunk, DatePart::WeekDay)
}

pub fn eval_dayname(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_part_string(arena, args, chunk, DatePart::DayName)
}

pub fn eval_hour(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_part_int(arena, expr, args, chunk, DatePart::Hour)
}

pub fn eval_minute(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_part_int(arena, expr, args, chunk, DatePart::Minute)
}

pub fn eval_month(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_part_int(arena, expr, args, chunk, DatePart::Month)
}

pub fn eval_monthname(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_part_string(arena, args, chunk, DatePart::MonthName)
}

pub fn eval_quarter(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_part_int(arena, expr, args, chunk, DatePart::Quarter)
}

pub fn eval_second(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_part_int(arena, expr, args, chunk, DatePart::Second)
}

pub fn eval_week(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_part_int(arena, expr, args, chunk, DatePart::Week)
}

pub fn eval_weekofyear(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_part_int(arena, expr, args, chunk, DatePart::Week)
}

pub fn eval_year(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_part_int(arena, expr, args, chunk, DatePart::Year)
}

pub fn eval_yearweek(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let arr = arena.eval(args[0], chunk)?;
    let dts = super::common::extract_datetime_array(&arr)?;
    let mut out = Vec::with_capacity(dts.len());
    for dt in dts {
        let v = dt.map(|d| {
            let iso = d.iso_week();
            (iso.year() as i64) * 100 + (iso.week() as i64)
        });
        out.push(v);
    }
    let out = Arc::new(Int64Array::from(out)) as ArrayRef;
    let expected_type = arena.data_type(expr).cloned().unwrap_or(DataType::Int64);
    if out.data_type() == &expected_type {
        return Ok(out);
    }
    cast(&out, &expected_type).map_err(|e| {
        format!(
            "date part output cast failed from {:?} to {:?}: {}",
            out.data_type(),
            expected_type,
            e
        )
    })
}

#[cfg(test)]
mod tests {
    use crate::exec::expr::function::date::test_utils::assert_date_function_logic;

    #[test]
    fn test_day_logic() {
        assert_date_function_logic("day");
    }

    #[test]
    fn test_dayofmonth_logic() {
        assert_date_function_logic("dayofmonth");
    }

    #[test]
    fn test_dayofweek_logic() {
        assert_date_function_logic("dayofweek");
    }

    #[test]
    fn test_dayofyear_logic() {
        assert_date_function_logic("dayofyear");
    }

    #[test]
    fn test_dayofweek_iso_logic() {
        assert_date_function_logic("dayofweek_iso");
    }

    #[test]
    fn test_weekday_logic() {
        assert_date_function_logic("weekday");
    }

    #[test]
    fn test_dayname_logic() {
        assert_date_function_logic("dayname");
    }

    #[test]
    fn test_hour_logic() {
        assert_date_function_logic("hour");
    }

    #[test]
    fn test_minute_logic() {
        assert_date_function_logic("minute");
    }

    #[test]
    fn test_month_logic() {
        assert_date_function_logic("month");
    }

    #[test]
    fn test_monthname_logic() {
        assert_date_function_logic("monthname");
    }

    #[test]
    fn test_quarter_logic() {
        assert_date_function_logic("quarter");
    }

    #[test]
    fn test_second_logic() {
        assert_date_function_logic("second");
    }

    #[test]
    fn test_week_logic() {
        assert_date_function_logic("week");
    }

    #[test]
    fn test_weekofyear_logic() {
        assert_date_function_logic("weekofyear");
    }

    #[test]
    fn test_year_logic() {
        assert_date_function_logic("year");
    }
}
