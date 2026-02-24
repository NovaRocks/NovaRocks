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

fn datetime_tail_value(dt: NaiveDateTime) -> i64 {
    dt.day() as i64 * 1_000_000_000_000
        + dt.hour() as i64 * 10_000_000_000
        + dt.minute() as i64 * 100_000_000
        + dt.second() as i64 * 1_000_000
        + (dt.nanosecond() / 1_000) as i64
}

fn months_diff_starrocks(lhs: NaiveDateTime, rhs: NaiveDateTime) -> i64 {
    let mut month =
        (lhs.year() - rhs.year()) as i64 * 12 + (lhs.month() as i64 - rhs.month() as i64);
    let lhs_tail = datetime_tail_value(lhs);
    let rhs_tail = datetime_tail_value(rhs);

    if month >= 0 {
        if lhs_tail < rhs_tail {
            month -= 1;
        }
    } else if lhs_tail > rhs_tail {
        month += 1;
    }

    month
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
                if day2 != last_day_of_month2 {
                    diff -= 1;
                } else if day1 == last_day_of_month1 && us_of_day1 > us_of_day2 {
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
        if day2 != last_day_of_month2 {
            diff -= 1;
        } else if day1 == last_day_of_month1 && us_of_day1 > us_of_day2 {
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
    let diff = lhs - rhs;
    match unit {
        DiffUnit::Milliseconds => diff.num_milliseconds(),
        DiffUnit::Seconds => diff.num_seconds(),
        DiffUnit::Minutes => diff.num_minutes(),
        DiffUnit::Hours => diff.num_hours(),
        DiffUnit::Days => diff.num_days(),
        DiffUnit::Weeks => diff.num_weeks(),
        DiffUnit::Months => months_diff_starrocks(lhs, rhs),
        DiffUnit::Years => {
            let (sign, start, end) = if lhs >= rhs {
                (1_i64, rhs, lhs)
            } else {
                (-1_i64, lhs, rhs)
            };
            let mut years = (end.year() - start.year()) as i64;
            let end_tuple = (end.month(), end.day(), end.time());
            let start_tuple = (start.month(), start.day(), start.time());
            if end_tuple < start_tuple {
                years -= 1;
            }
            years * sign
        }
        DiffUnit::Quarters => months_diff_starrocks(lhs, rhs) / 3,
    }
}

#[inline]
fn eval_diff_unit(
    arena: &ExprArena,
    args: &[ExprId],
    chunk: &Chunk,
    unit: DiffUnit,
) -> Result<ArrayRef, String> {
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

pub fn eval_datediff(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let a = extract_date_array(&arena.eval(args[0], chunk)?)?;
    let b = extract_date_array(&arena.eval(args[1], chunk)?)?;
    let mut out = Vec::with_capacity(a.len());
    for i in 0..a.len() {
        let v = match (a[i], b[i]) {
            (Some(x), Some(y)) => Some((x - y).num_days()),
            _ => None,
        };
        out.push(v.map(|v| v as i64));
    }
    Ok(Arc::new(Int64Array::from(out)) as ArrayRef)
}

pub fn eval_date_diff(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
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
                "type column should be one of day/hour/minute/second/millisecond".to_string(),
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

pub fn eval_days_diff(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_datediff(arena, expr, args, chunk)
}

pub fn eval_seconds_diff(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_diff_unit(arena, args, chunk, DiffUnit::Seconds)
}

pub fn eval_milliseconds_diff(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_diff_unit(arena, args, chunk, DiffUnit::Milliseconds)
}

pub fn eval_minutes_diff(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_diff_unit(arena, args, chunk, DiffUnit::Minutes)
}

pub fn eval_hours_diff(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_diff_unit(arena, args, chunk, DiffUnit::Hours)
}

pub fn eval_weeks_diff(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_diff_unit(arena, args, chunk, DiffUnit::Weeks)
}

pub fn eval_months_diff(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_diff_unit(arena, args, chunk, DiffUnit::Months)
}

pub fn eval_quarters_diff(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_diff_unit(arena, args, chunk, DiffUnit::Quarters)
}

pub fn eval_years_diff(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_diff_unit(arena, args, chunk, DiffUnit::Years)
}

#[cfg(test)]
mod tests {
    use crate::exec::expr::function::date::test_utils::assert_date_function_logic;

    #[test]
    fn test_datediff_logic() {
        assert_date_function_logic("datediff");
    }

    #[test]
    fn test_days_diff_logic() {
        assert_date_function_logic("days_diff");
    }

    #[test]
    fn test_seconds_diff_logic() {
        assert_date_function_logic("seconds_diff");
    }

    #[test]
    fn test_milliseconds_diff_logic() {
        assert_date_function_logic("milliseconds_diff");
    }

    #[test]
    fn test_minutes_diff_logic() {
        assert_date_function_logic("minutes_diff");
    }

    #[test]
    fn test_hours_diff_logic() {
        assert_date_function_logic("hours_diff");
    }

    #[test]
    fn test_weeks_diff_logic() {
        assert_date_function_logic("weeks_diff");
    }

    #[test]
    fn test_months_diff_logic() {
        assert_date_function_logic("months_diff");
    }

    #[test]
    fn test_quarters_diff_logic() {
        assert_date_function_logic("quarters_diff");
    }

    #[test]
    fn test_years_diff_logic() {
        assert_date_function_logic("years_diff");
    }
}
