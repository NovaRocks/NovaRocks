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
use super::common::{extract_datetime_array, naive_to_date32, to_timestamp_value};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{Array, ArrayRef, Date32Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, Timelike};
use std::sync::Arc;

#[derive(Clone, Copy)]
enum DateTruncUnit {
    Microsecond,
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

fn parse_date_trunc_unit(unit: &str) -> Result<DateTruncUnit, String> {
    match unit {
        "microsecond" => Ok(DateTruncUnit::Microsecond),
        "millisecond" => Ok(DateTruncUnit::Millisecond),
        "second" => Ok(DateTruncUnit::Second),
        "minute" => Ok(DateTruncUnit::Minute),
        "hour" => Ok(DateTruncUnit::Hour),
        "day" => Ok(DateTruncUnit::Day),
        "week" => Ok(DateTruncUnit::Week),
        "month" => Ok(DateTruncUnit::Month),
        "quarter" => Ok(DateTruncUnit::Quarter),
        "year" => Ok(DateTruncUnit::Year),
        _ => Err(
            "format value must in {microsecond, millisecond, second, minute, hour, day, month, year, week, quarter}"
                .to_string(),
        ),
    }
}

fn apply_date_trunc_unit(dt: NaiveDateTime, unit: DateTruncUnit) -> NaiveDateTime {
    match unit {
        DateTruncUnit::Microsecond => dt,
        DateTruncUnit::Millisecond => {
            let nanos = dt.nanosecond();
            dt.with_nanosecond((nanos / 1_000_000) * 1_000_000).unwrap()
        }
        DateTruncUnit::Second => dt.with_nanosecond(0).unwrap(),
        DateTruncUnit::Minute => dt
            .with_second(0)
            .and_then(|v| v.with_nanosecond(0))
            .unwrap(),
        DateTruncUnit::Hour => dt
            .with_minute(0)
            .and_then(|v| v.with_second(0))
            .and_then(|v| v.with_nanosecond(0))
            .unwrap(),
        DateTruncUnit::Day => dt.date().and_hms_opt(0, 0, 0).unwrap(),
        DateTruncUnit::Week => {
            let weekday = dt.weekday().num_days_from_monday() as i64;
            (dt - Duration::days(weekday))
                .date()
                .and_hms_opt(0, 0, 0)
                .unwrap()
        }
        DateTruncUnit::Month => NaiveDate::from_ymd_opt(dt.year(), dt.month(), 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
        DateTruncUnit::Quarter => {
            let q = (dt.month() - 1) / 3;
            NaiveDate::from_ymd_opt(dt.year(), q * 3 + 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
        }
        DateTruncUnit::Year => NaiveDate::from_ymd_opt(dt.year(), 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
    }
}

#[inline]
fn eval_date_trunc_inner(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let unit_arr = arena.eval(args[0], chunk)?;
    let unit_arr = unit_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "date_trunc expects string unit".to_string())?;
    let date_arr = arena.eval(args[1], chunk)?;
    let dts = extract_datetime_array(&date_arr)?;
    let output_type = arena
        .data_type(expr)
        .cloned()
        .unwrap_or(DataType::Timestamp(TimeUnit::Microsecond, None));
    let mut out = Vec::with_capacity(dts.len());
    for (i, dt) in dts.into_iter().enumerate() {
        if unit_arr.is_null(i) {
            out.push(None);
            continue;
        }
        let unit = parse_date_trunc_unit(&unit_arr.value(i).to_lowercase())?;
        let truncated = dt.map(|d| apply_date_trunc_unit(d, unit));
        let v = match output_type {
            DataType::Date32 => truncated.map(|d| naive_to_date32(d.date()) as i64),
            _ => truncated.and_then(|d| to_timestamp_value(d, &output_type).ok()),
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

pub fn eval_date_trunc(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_date_trunc_inner(arena, expr, args, chunk)
}

pub fn eval_date_floor(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_date_trunc_inner(arena, expr, args, chunk)
}

pub fn eval_alignment_timestamp(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() != 2 {
        return Err("alignment_timestamp expects 2 args".to_string());
    }
    eval_date_trunc_inner(arena, expr, args, chunk)
}

#[cfg(test)]
mod tests {
    use crate::exec::expr::function::date::test_utils::assert_date_function_logic;

    #[test]
    fn test_date_trunc_logic() {
        assert_date_function_logic("date_trunc");
    }

    #[test]
    fn test_date_floor_logic() {
        assert_date_function_logic("date_floor");
    }

    #[test]
    fn test_alignment_timestamp_logic() {
        assert_date_function_logic("alignment_timestamp");
    }
}
