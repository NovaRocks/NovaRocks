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
use super::common::{extract_date_array, extract_datetime_array};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use chrono::{Datelike, NaiveDateTime, Timelike};
use novarocks_functions::builtin::calendar_extended_shared::{
    CalendarDurationUnit, CalendarOperation, calendar_duration_difference, evaluate_legacy_calendar,
};
use novarocks_functions::calendar_period_numeric::{months_diff_starrocks, years_diff_starrocks};
use std::sync::Arc;

#[derive(Clone, Copy)]
enum DiffUnit {
    Milliseconds,
    Seconds,
    Minutes,
    Hours,
    Days,
    Weeks,
    Months,
    Years,
    Quarters,
}

fn parse_diff_unit(unit: &str) -> Option<DiffUnit> {
    match unit {
        "year" => Some(DiffUnit::Years),
        "quarter" => Some(DiffUnit::Quarters),
        "month" => Some(DiffUnit::Months),
        "week" => Some(DiffUnit::Weeks),
        "day" => Some(DiffUnit::Days),
        "hour" => Some(DiffUnit::Hours),
        "minute" => Some(DiffUnit::Minutes),
        "second" => Some(DiffUnit::Seconds),
        "millisecond" => Some(DiffUnit::Milliseconds),
        _ => None,
    }
}

#[inline]
fn micros_of_day(dt: NaiveDateTime) -> i64 {
    dt.hour() as i64 * 3_600_000_000
        + dt.minute() as i64 * 60_000_000
        + dt.second() as i64 * 1_000_000
        + (dt.nanosecond() / 1_000) as i64
}

#[inline]
fn last_day_of_month(year: i32, month: u32) -> u32 {
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    let first_next =
        chrono::NaiveDate::from_ymd_opt(next_year, next_month, 1).expect("valid month boundary");
    (first_next - chrono::Duration::days(1)).day()
}

fn years_diff_v2(lhs: NaiveDateTime, rhs: NaiveDateTime) -> i64 {
    let (from, to, sign) = if rhs < lhs {
        (rhs, lhs, 1_i64)
    } else {
        (lhs, rhs, -1_i64)
    };

    let year1 = from.year();
    let month1 = from.month();
    let day1 = from.day();
    let us_of_day1 = micros_of_day(from);
    let last_day_of_month1 = last_day_of_month(year1, month1);

    let year2 = to.year();
    let month2 = to.month();
    let day2 = to.day();
    let us_of_day2 = micros_of_day(to);
    let last_day_of_month2 = last_day_of_month(year2, month2);

    let mut diff = (year2 - year1) as i64;

    if month1 > month2 {
        diff -= 1;
    } else if month1 == month2 {
        if last_day_of_month1 != last_day_of_month2 {
            if day1 > day2 {
                if day2 != last_day_of_month2
                    || (day1 == last_day_of_month1 && us_of_day1 > us_of_day2)
                {
                    diff -= 1;
                }
            } else if day1 == day2 && day2 != last_day_of_month2 && us_of_day1 > us_of_day2 {
                diff -= 1;
            }
        } else if day1 > day2 || (day1 == day2 && us_of_day1 > us_of_day2) {
            diff -= 1;
        }
    }

    diff * sign
}

fn months_diff_v2(lhs: NaiveDateTime, rhs: NaiveDateTime) -> i64 {
    let (from, to, sign) = if rhs < lhs {
        (rhs, lhs, 1_i64)
    } else {
        (lhs, rhs, -1_i64)
    };

    let year1 = from.year();
    let month1 = from.month();
    let day1 = from.day();
    let us_of_day1 = micros_of_day(from);
    let last_day_of_month1 = last_day_of_month(year1, month1);

    let year2 = to.year();
    let month2 = to.month();
    let day2 = to.day();
    let us_of_day2 = micros_of_day(to);
    let last_day_of_month2 = last_day_of_month(year2, month2);

    let mut diff = ((year2 - year1) as i64) * 12 + (month2 as i64 - month1 as i64);

    if day1 > day2 {
        if day2 != last_day_of_month2 || (day1 == last_day_of_month1 && us_of_day1 > us_of_day2) {
            diff -= 1;
        }
    } else if day1 == day2 {
        if day2 == last_day_of_month2 {
            if day1 == last_day_of_month1 && us_of_day1 > us_of_day2 {
                diff -= 1;
            }
        } else if us_of_day1 > us_of_day2 {
            diff -= 1;
        }
    }

    sign * diff
}

#[inline]
fn eval_diff_value(lhs: NaiveDateTime, rhs: NaiveDateTime, unit: DiffUnit) -> i64 {
    match unit {
        DiffUnit::Milliseconds => {
            calendar_duration_difference(lhs, rhs, CalendarDurationUnit::Milliseconds)
        }
        DiffUnit::Seconds => calendar_duration_difference(lhs, rhs, CalendarDurationUnit::Seconds),
        DiffUnit::Minutes => calendar_duration_difference(lhs, rhs, CalendarDurationUnit::Minutes),
        DiffUnit::Hours => calendar_duration_difference(lhs, rhs, CalendarDurationUnit::Hours),
        DiffUnit::Days => calendar_duration_difference(lhs, rhs, CalendarDurationUnit::Days),
        DiffUnit::Weeks => calendar_duration_difference(lhs, rhs, CalendarDurationUnit::Weeks),
        DiffUnit::Months => months_diff_starrocks(lhs, rhs),
        DiffUnit::Years => years_diff_starrocks(lhs, rhs),
        DiffUnit::Quarters => months_diff_starrocks(lhs, rhs) / 3,
    }
}

#[inline]
fn eval_diff_unit<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    args: &[ExprId],
    chunk: &Chunk,
    unit: DiffUnit,
) -> Result<ArrayRef, __Eval::Error> {
    let a = extract_datetime_array(&arena.eval(args[0], chunk)?)?;
    let b = extract_datetime_array(&arena.eval(args[1], chunk)?)?;
    let mut out = Vec::with_capacity(a.len());
    for i in 0..a.len() {
        let v = match (a[i], b[i]) {
            (Some(x), Some(y)) => Some(eval_diff_value(x, y, unit)),
            _ => None,
        };
        out.push(v);
    }
    Ok(Arc::new(Int64Array::from(out)) as ArrayRef)
}

fn eval_shared_duration<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    args: &[ExprId],
    chunk: &Chunk,
    operation: CalendarOperation,
) -> Result<ArrayRef, __Eval::Error> {
    let left = arena.eval(args[0], chunk)?;
    let right = arena.eval(args[1], chunk)?;
    let rows = left.len();
    Ok((evaluate_legacy_calendar(
        operation,
        &[left, right],
        &arrow::datatypes::DataType::Int64,
        rows,
    ))?)
}

pub fn eval_datediff<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let a = extract_date_array(&arena.eval(args[0], chunk)?)?;
    let b = extract_date_array(&arena.eval(args[1], chunk)?)?;
    let mut out = Vec::with_capacity(a.len());
    for i in 0..a.len() {
        let v = novarocks_functions::calendar_period_numeric::date_difference_days(a[i], b[i]);
        out.push(v);
    }
    Ok(Arc::new(Int64Array::from(out)) as ArrayRef)
}

pub fn eval_date_diff<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let unit_arr = arena.eval(args[0], chunk)?;
    let unit_arr = unit_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "date_diff expects unit string".to_string())?;
    let lhs = extract_datetime_array(&arena.eval(args[1], chunk)?)?;
    let rhs = extract_datetime_array(&arena.eval(args[2], chunk)?)?;

    let mut out = Vec::with_capacity(lhs.len());
    for i in 0..lhs.len() {
        if unit_arr.is_null(i) {
            out.push(None);
            continue;
        }

        let Some(unit) = parse_diff_unit(&unit_arr.value(i).to_lowercase()) else {
            return Err(
                ("type column should be one of day/hour/minute/second/millisecond".to_string())
                    .into(),
            );
        };

        let v = match (lhs[i], rhs[i]) {
            (Some(x), Some(y)) => {
                let value = match unit {
                    DiffUnit::Years => years_diff_v2(x, y),
                    DiffUnit::Months => months_diff_v2(x, y),
                    DiffUnit::Quarters => months_diff_v2(x, y) / 3,
                    _ => eval_diff_value(x, y, unit),
                };
                Some(value)
            }
            _ => None,
        };
        out.push(v);
    }

    Ok(Arc::new(Int64Array::from(out)) as ArrayRef)
}

pub fn eval_days_diff<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_datediff(arena, expr, args, chunk))?)
}

pub fn eval_seconds_diff<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_shared_duration(arena, args, chunk, CalendarOperation::SecondsDiff))?)
}

pub fn eval_milliseconds_diff<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_diff_unit(arena, args, chunk, DiffUnit::Milliseconds))?)
}

pub fn eval_minutes_diff<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_shared_duration(arena, args, chunk, CalendarOperation::MinutesDiff))?)
}

pub fn eval_hours_diff<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_shared_duration(arena, args, chunk, CalendarOperation::HoursDiff))?)
}

pub fn eval_weeks_diff<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_shared_duration(arena, args, chunk, CalendarOperation::WeeksDiff))?)
}

pub fn eval_months_diff<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_diff_unit(arena, args, chunk, DiffUnit::Months))?)
}

pub fn eval_quarters_diff<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_diff_unit(arena, args, chunk, DiffUnit::Quarters))?)
}

pub fn eval_years_diff<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_diff_unit(arena, args, chunk, DiffUnit::Years))?)
}
