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
#![allow(unused_imports)]

use crate::common;
use arrow::array::{
    Array, ArrayRef, Date32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{Local, NaiveDate, NaiveDateTime, TimeZone, Timelike};
use novarocks::exec::expr::function::date::eval_date_function;
use novarocks::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Helpers adapted from src/exec/expr/function/date/test_utils.rs
// ---------------------------------------------------------------------------

fn dt_micros(s: &str) -> i64 {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
        .unwrap()
        .and_utc()
        .timestamp_micros()
}

fn date32_from_ymd(year: i32, month: u32, day: u32) -> i32 {
    let date = NaiveDate::from_ymd_opt(year, month, day).unwrap();
    let unix_epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    (date - unix_epoch).num_days() as i32
}

fn date_eval_ts(
    name: &str,
    arena: &novarocks::exec::expr::ExprArena,
    expr: novarocks::exec::expr::ExprId,
    args: &[novarocks::exec::expr::ExprId],
    chunk: &novarocks::exec::chunk::Chunk,
) -> i64 {
    let arr = eval_date_function(name, arena, expr, args, chunk).unwrap();
    let arr = arr
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    arr.value(0)
}

fn date_eval_i64(
    name: &str,
    arena: &novarocks::exec::expr::ExprArena,
    expr: novarocks::exec::expr::ExprId,
    args: &[novarocks::exec::expr::ExprId],
    chunk: &novarocks::exec::chunk::Chunk,
) -> i64 {
    let arr = eval_date_function(name, arena, expr, args, chunk).unwrap();
    let arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
    arr.value(0)
}

fn date_eval_date32(
    name: &str,
    arena: &novarocks::exec::expr::ExprArena,
    expr: novarocks::exec::expr::ExprId,
    args: &[novarocks::exec::expr::ExprId],
    chunk: &novarocks::exec::chunk::Chunk,
) -> i32 {
    let arr = eval_date_function(name, arena, expr, args, chunk).unwrap();
    let arr = arr.as_any().downcast_ref::<Date32Array>().unwrap();
    arr.value(0)
}

fn date_eval_str(
    name: &str,
    arena: &novarocks::exec::expr::ExprArena,
    expr: novarocks::exec::expr::ExprId,
    args: &[novarocks::exec::expr::ExprId],
    chunk: &novarocks::exec::chunk::Chunk,
) -> String {
    let arr = eval_date_function(name, arena, expr, args, chunk).unwrap();
    let arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
    arr.value(0).to_string()
}

fn assert_date_function_logic(name: &str) {
    let canonical = match name {
        "adddate" => "date_add",
        "curdate" => "current_date",
        "curtime" => "current_time",
        "day_of_week_iso" => "dayofweek_iso",
        "date_to_iso8601" | "datetime_to_iso8601" => "to_iso8601",
        "dayofmonth" => "day",
        "localtime" | "localtimestamp" | "now" => "current_timestamp",
        "str2date" => "str_to_date",
        "strftime" => "date_format",
        "subdate" => "date_sub",
        "week_day" => "weekday",
        "week_iso" => "week",
        "weekofyear" => "week",
        other => other,
    };

    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_ts = common::typed_null(&mut arena, DataType::Timestamp(TimeUnit::Microsecond, None));
    let expr_i64 = common::typed_null(&mut arena, DataType::Int64);
    let expr_date = common::typed_null(&mut arena, DataType::Date32);
    let expr_str = common::typed_null(&mut arena, DataType::Utf8);

    match canonical {
        "current_date" => {
            let arr = eval_date_function(name, &arena, expr_date, &[], &chunk).unwrap();
            assert_eq!(arr.len(), 1);
            assert!(!arr.is_null(0));
        }
        "current_time" | "current_timestamp" | "utc_time" | "utc_timestamp" => {
            let arr = eval_date_function(name, &arena, expr_ts, &[], &chunk).unwrap();
            assert_eq!(arr.len(), 1);
            assert!(!arr.is_null(0));
        }
        "day" => {
            let dt = common::literal_string(&mut arena, "2020-01-02 03:04:05");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[dt], &chunk), 2);
        }
        "dayname" => {
            let dt = common::literal_string(&mut arena, "2020-01-02 03:04:05");
            assert_eq!(
                date_eval_str(name, &arena, expr_str, &[dt], &chunk),
                "Thursday"
            );
        }
        "dayofweek" => {
            let dt = common::literal_string(&mut arena, "2020-01-02 03:04:05");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[dt], &chunk), 5);
        }
        "dayofweek_iso" => {
            let dt = common::literal_string(&mut arena, "2020-01-02 03:04:05");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[dt], &chunk), 4);
        }
        "dayofyear" => {
            let dt = common::literal_string(&mut arena, "2020-01-02 03:04:05");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[dt], &chunk), 2);
        }
        "weekday" => {
            let dt = common::literal_string(&mut arena, "2020-01-02 03:04:05");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[dt], &chunk), 3);
        }
        "month" => {
            let dt = common::literal_string(&mut arena, "2020-01-02 03:04:05");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[dt], &chunk), 1);
        }
        "monthname" => {
            let dt = common::literal_string(&mut arena, "2020-01-02 03:04:05");
            assert_eq!(
                date_eval_str(name, &arena, expr_str, &[dt], &chunk),
                "January"
            );
        }
        "quarter" => {
            let dt = common::literal_string(&mut arena, "2020-01-02 03:04:05");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[dt], &chunk), 1);
        }
        "week" => {
            let dt = common::literal_string(&mut arena, "2020-01-02 03:04:05");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[dt], &chunk), 1);
        }
        "year" => {
            let dt = common::literal_string(&mut arena, "2020-01-02 03:04:05");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[dt], &chunk), 2020);
        }
        "hour" => {
            let dt = common::literal_string(&mut arena, "2020-01-02 03:04:05");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[dt], &chunk), 3);
        }
        "minute" => {
            let dt = common::literal_string(&mut arena, "2020-01-02 03:04:05");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[dt], &chunk), 4);
        }
        "second" => {
            let dt = common::literal_string(&mut arena, "2020-01-02 03:04:05");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[dt], &chunk), 5);
        }
        "datediff" | "days_diff" => {
            let d1 = common::literal_string(&mut arena, "2020-01-03 00:00:00");
            let d2 = common::literal_string(&mut arena, "2020-01-01 00:00:00");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[d1, d2], &chunk), 2);
        }
        "weeks_diff" => {
            let d1 = common::literal_string(&mut arena, "2020-01-15 00:00:00");
            let d2 = common::literal_string(&mut arena, "2020-01-01 00:00:00");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[d1, d2], &chunk), 2);
        }
        "months_diff" => {
            let d1 = common::literal_string(&mut arena, "2020-03-15 00:00:00");
            let d2 = common::literal_string(&mut arena, "2020-01-15 00:00:00");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[d1, d2], &chunk), 2);
        }
        "years_diff" => {
            let d1 = common::literal_string(&mut arena, "2021-01-15 00:00:00");
            let d2 = common::literal_string(&mut arena, "2020-01-15 00:00:00");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[d1, d2], &chunk), 1);
            let leap_l = common::literal_string(&mut arena, "2026-02-28");
            let leap_r = common::literal_string(&mut arena, "2024-02-29");
            assert_eq!(
                date_eval_i64(name, &arena, expr_i64, &[leap_l, leap_r], &chunk),
                1
            );
        }
        "hours_diff" => {
            let d1 = common::literal_string(&mut arena, "2020-01-01 03:00:00");
            let d2 = common::literal_string(&mut arena, "2020-01-01 01:00:00");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[d1, d2], &chunk), 2);
        }
        "minutes_diff" => {
            let d1 = common::literal_string(&mut arena, "2020-01-01 00:02:00");
            let d2 = common::literal_string(&mut arena, "2020-01-01 00:01:00");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[d1, d2], &chunk), 1);
        }
        "seconds_diff" => {
            let d1 = common::literal_string(&mut arena, "2020-01-01 00:00:03");
            let d2 = common::literal_string(&mut arena, "2020-01-01 00:00:01");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[d1, d2], &chunk), 2);
        }
        "quarters_diff" => {
            let d1 = common::literal_string(&mut arena, "2020-05-01 00:00:00");
            let d2 = common::literal_string(&mut arena, "2020-02-01 00:00:00");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[d1, d2], &chunk), 1);
        }
        "date" | "to_date" | "to_tera_date" => {
            let dt = common::literal_string(&mut arena, "2020-01-02 03:04:05");
            assert_eq!(
                date_eval_date32(name, &arena, expr_date, &[dt], &chunk),
                date32_from_ymd(2020, 1, 2)
            );
        }
        "timestamp" | "to_datetime" => {
            let dt = common::literal_string(&mut arena, "2020-01-02 03:04:05");
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[dt], &chunk),
                dt_micros("2020-01-02 03:04:05")
            );
        }
        "date_add" | "days_add" => {
            let base = common::literal_string(&mut arena, "2020-01-01 00:00:00");
            let n = common::literal_i64(&mut arena, 2);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-01-03 00:00:00")
            );
        }
        "date_sub" | "days_sub" => {
            let base = common::literal_string(&mut arena, "2020-01-03 00:00:00");
            let n = common::literal_i64(&mut arena, 2);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-01-01 00:00:00")
            );
        }
        "weeks_add" => {
            let base = common::literal_string(&mut arena, "2020-01-01 00:00:00");
            let n = common::literal_i64(&mut arena, 1);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-01-08 00:00:00")
            );
        }
        "weeks_sub" => {
            let base = common::literal_string(&mut arena, "2020-01-08 00:00:00");
            let n = common::literal_i64(&mut arena, 1);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-01-01 00:00:00")
            );
        }
        "hours_add" => {
            let base = common::literal_string(&mut arena, "2020-01-01 01:00:00");
            let n = common::literal_i64(&mut arena, 2);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-01-01 03:00:00")
            );
        }
        "hours_sub" => {
            let base = common::literal_string(&mut arena, "2020-01-01 03:00:00");
            let n = common::literal_i64(&mut arena, 2);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-01-01 01:00:00")
            );
        }
        "minutes_add" => {
            let base = common::literal_string(&mut arena, "2020-01-01 00:01:00");
            let n = common::literal_i64(&mut arena, 1);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-01-01 00:02:00")
            );
        }
        "minutes_sub" => {
            let base = common::literal_string(&mut arena, "2020-01-01 00:02:00");
            let n = common::literal_i64(&mut arena, 1);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-01-01 00:01:00")
            );
        }
        "seconds_add" => {
            let base = common::literal_string(&mut arena, "2020-01-01 00:00:01");
            let n = common::literal_i64(&mut arena, 2);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-01-01 00:00:03")
            );
        }
        "seconds_sub" => {
            let base = common::literal_string(&mut arena, "2020-01-01 00:00:03");
            let n = common::literal_i64(&mut arena, 2);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-01-01 00:00:01")
            );
        }
        "milliseconds_add" => {
            let base = common::literal_string(&mut arena, "2020-01-01 00:00:00");
            let n = common::literal_i64(&mut arena, 1500);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-01-01 00:00:01") + 500_000
            );
        }
        "milliseconds_diff" => {
            let d1 = common::literal_string(&mut arena, "2020-01-01 00:00:03");
            let d2 = common::literal_string(&mut arena, "2020-01-01 00:00:01");
            assert_eq!(
                date_eval_i64(name, &arena, expr_i64, &[d1, d2], &chunk),
                2000
            );
        }
        "milliseconds_sub" => {
            let base = common::literal_string(&mut arena, "2020-01-01 00:00:02");
            let n = common::literal_i64(&mut arena, 1500);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-01-01 00:00:00") + 500_000
            );
        }
        "microseconds_add" => {
            let base = common::literal_string(&mut arena, "2020-01-01 00:00:00");
            let n = common::literal_i64(&mut arena, 1000);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-01-01 00:00:00") + 1000
            );
        }
        "microseconds_sub" => {
            let base = common::literal_string(&mut arena, "2020-01-01 00:00:01");
            let n = common::literal_i64(&mut arena, 1000);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-01-01 00:00:01") - 1000
            );
        }
        "add_months" | "months_add" => {
            let base = common::literal_string(&mut arena, "2020-01-15 00:00:00");
            let n = common::literal_i64(&mut arena, 2);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-03-15 00:00:00")
            );
        }
        "months_sub" => {
            let base = common::literal_string(&mut arena, "2020-03-15 00:00:00");
            let n = common::literal_i64(&mut arena, 1);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-02-15 00:00:00")
            );
        }
        "years_add" => {
            let base = common::literal_string(&mut arena, "2020-01-15 00:00:00");
            let n = common::literal_i64(&mut arena, 1);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2021-01-15 00:00:00")
            );
        }
        "years_sub" => {
            let base = common::literal_string(&mut arena, "2021-01-15 00:00:00");
            let n = common::literal_i64(&mut arena, 1);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-01-15 00:00:00")
            );
        }
        "quarters_add" => {
            let base = common::literal_string(&mut arena, "2020-01-15 00:00:00");
            let n = common::literal_i64(&mut arena, 1);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-04-15 00:00:00")
            );
        }
        "quarters_sub" => {
            let base = common::literal_string(&mut arena, "2020-04-15 00:00:00");
            let n = common::literal_i64(&mut arena, 1);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-01-15 00:00:00")
            );
        }
        "date_format" => {
            let dt = common::literal_string(&mut arena, "2020-01-02 03:04:05");
            let fmt = common::literal_string(&mut arena, "%Y-%m-%d");
            assert_eq!(
                date_eval_str(name, &arena, expr_str, &[dt, fmt], &chunk),
                "2020-01-02"
            );
        }
        "time_format" => {
            let dt = common::literal_string(&mut arena, "12:34:56");
            let fmt = common::literal_string(&mut arena, "%H:%i:%s");
            assert_eq!(
                date_eval_str(name, &arena, expr_str, &[dt, fmt], &chunk),
                "00:00:00"
            );
        }
        "str_to_date" => {
            let s = common::literal_string(&mut arena, "2020-01-02");
            let fmt = common::literal_string(&mut arena, "%Y-%m-%d");
            assert_eq!(
                date_eval_date32(name, &arena, expr_date, &[s, fmt], &chunk),
                date32_from_ymd(2020, 1, 2)
            );
        }
        "date_trunc" | "date_floor" => {
            let unit = common::literal_string(&mut arena, "month");
            let dt = common::literal_string(&mut arena, "2020-01-15 12:34:56");
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[unit, dt], &chunk),
                dt_micros("2020-01-01 00:00:00")
            );
        }
        "alignment_timestamp" => {
            let unit = common::literal_string(&mut arena, "day");
            let dt = common::literal_string(&mut arena, "2020-01-15 12:34:56");
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[unit, dt], &chunk),
                dt_micros("2020-01-15 00:00:00")
            );
        }
        "time_slice" | "date_slice" => {
            let dt = common::literal_string(&mut arena, "2020-01-02 03:04:05");
            let interval = common::literal_i64(&mut arena, 1);
            let unit = common::literal_string(&mut arena, "hour");
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[dt, interval, unit], &chunk),
                dt_micros("2020-01-02 03:00:00")
            );
        }
        "timestampadd" => {
            let unit = common::literal_string(&mut arena, "day");
            let interval = common::literal_i64(&mut arena, 2);
            let dt = common::literal_string(&mut arena, "2020-01-01 00:00:00");
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[unit, interval, dt], &chunk),
                dt_micros("2020-01-03 00:00:00")
            );
        }
        "timestampdiff" => {
            let unit = common::literal_string(&mut arena, "hour");
            let dt1 = common::literal_string(&mut arena, "2020-01-01 00:00:00");
            let dt2 = common::literal_string(&mut arena, "2020-01-01 03:00:00");
            assert_eq!(
                date_eval_i64(name, &arena, expr_i64, &[unit, dt1, dt2], &chunk),
                3
            );
        }
        "timediff" => {
            let t1 = common::literal_string(&mut arena, "2020-01-02 03:00:00");
            let t2 = common::literal_string(&mut arena, "2020-01-02 01:00:00");
            assert_eq!(
                date_eval_str(name, &arena, expr_str, &[t1, t2], &chunk),
                "02:00:00"
            );
        }
        "time_to_sec" => {
            let t = common::literal_string(&mut arena, "01:01:01");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[t], &chunk), 3661);
        }
        "sec_to_time" => {
            let s = common::literal_i64(&mut arena, 3661);
            assert_eq!(
                date_eval_str(name, &arena, expr_str, &[s], &chunk),
                "01:01:01"
            );
        }
        "to_days" => {
            let d = common::literal_string(&mut arena, "1970-01-02");
            let out = date_eval_i64(name, &arena, expr_i64, &[d], &chunk);
            assert!(out > 0);
        }
        "to_iso8601" => {
            let d = common::literal_string(&mut arena, "2020-01-02 03:04:05.1234");
            assert_eq!(
                date_eval_str(name, &arena, expr_str, &[d], &chunk),
                "2020-01-02T03:04:05.123400"
            );
        }
        "from_days" => {
            let days = common::literal_i64(&mut arena, 719529);
            assert_eq!(
                date_eval_date32(name, &arena, expr_date, &[days], &chunk),
                date32_from_ymd(1970, 1, 2)
            );
        }
        "last_day" => {
            let d = common::literal_string(&mut arena, "2020-02-10");
            assert_eq!(
                date_eval_date32(name, &arena, expr_date, &[d], &chunk),
                date32_from_ymd(2020, 2, 29)
            );
        }
        "makedate" => {
            let y = common::literal_i64(&mut arena, 2020);
            let day = common::literal_i64(&mut arena, 32);
            assert_eq!(
                date_eval_date32(name, &arena, expr_date, &[y, day], &chunk),
                date32_from_ymd(2020, 2, 1)
            );
        }
        "next_day" => {
            let base = common::literal_string(&mut arena, "2020-01-02");
            let wd = common::literal_string(&mut arena, "Mon");
            assert_eq!(
                date_eval_date32(name, &arena, expr_date, &[base, wd], &chunk),
                date32_from_ymd(2020, 1, 6)
            );
        }
        "previous_day" => {
            let base = common::literal_string(&mut arena, "2020-01-02");
            let wd = common::literal_string(&mut arena, "Mon");
            assert_eq!(
                date_eval_date32(name, &arena, expr_date, &[base, wd], &chunk),
                date32_from_ymd(2019, 12, 30)
            );
        }
        "convert_tz" => {
            let dt = common::literal_string(&mut arena, "2020-01-01 00:00:00");
            let from = common::literal_string(&mut arena, "+00:00");
            let to = common::literal_string(&mut arena, "+08:00");
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[dt, from, to], &chunk),
                dt_micros("2020-01-01 08:00:00")
            );
        }
        "from_unixtime" => {
            let v = common::literal_i64(&mut arena, 1);
            let expected = Local
                .timestamp_opt(1, 0)
                .single()
                .map(|dt| dt.naive_local().and_utc().timestamp_micros())
                .unwrap();
            assert_eq!(date_eval_ts(name, &arena, expr_ts, &[v], &chunk), expected);
        }
        "from_unixtime_ms" => {
            let v = common::literal_i64(&mut arena, 1000);
            let expected = Local
                .timestamp_opt(1, 0)
                .single()
                .map(|dt| dt.naive_local().and_utc().timestamp_micros())
                .unwrap();
            assert_eq!(date_eval_ts(name, &arena, expr_ts, &[v], &chunk), expected);
        }
        "hour_from_unixtime" => {
            let v = common::literal_i64(&mut arena, 5 * 3600);
            let expected = Local
                .timestamp_opt(5 * 3600, 0)
                .single()
                .map(|dt| dt.hour() as i64)
                .unwrap();
            assert_eq!(
                date_eval_i64(name, &arena, expr_i64, &[v], &chunk),
                expected
            );
        }
        "unix_timestamp" => {
            let dt = common::literal_string(&mut arena, "1970-01-01 00:00:01");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[dt], &chunk), 1);
        }
        "substitute" => {
            let s = common::literal_string(&mut arena, "2020-01-02");
            assert_eq!(
                date_eval_str(name, &arena, expr_str, &[s], &chunk),
                "2020-01-02"
            );
        }
        other => panic!(
            "unsupported high-priority date function in helper: {}",
            other
        ),
    }
}

// ---------------------------------------------------------------------------
// Tests from add_months.rs
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Tests from convert_tz.rs
// ---------------------------------------------------------------------------

#[test]
fn test_convert_tz_logic() {
    assert_date_function_logic("convert_tz");
}

// ---------------------------------------------------------------------------
// Tests from current_date.rs
// ---------------------------------------------------------------------------

#[test]
fn test_current_date_logic() {
    assert_date_function_logic("current_date");
}

#[test]
fn test_curdate_logic() {
    assert_date_function_logic("curdate");
}

// ---------------------------------------------------------------------------
// Tests from current_time.rs
// ---------------------------------------------------------------------------

#[test]
fn test_current_time_logic() {
    assert_date_function_logic("current_time");
}

#[test]
fn test_curtime_logic() {
    assert_date_function_logic("curtime");
}

#[test]
fn test_utc_time_logic() {
    assert_date_function_logic("utc_time");
}

// ---------------------------------------------------------------------------
// Tests from current_timestamp.rs
// ---------------------------------------------------------------------------

#[test]
fn test_current_timestamp_logic() {
    assert_date_function_logic("current_timestamp");
}

#[test]
fn test_localtime_logic() {
    assert_date_function_logic("localtime");
}

#[test]
fn test_localtimestamp_logic() {
    assert_date_function_logic("localtimestamp");
}

#[test]
fn test_now_logic() {
    assert_date_function_logic("now");
}

#[test]
fn test_utc_timestamp_logic() {
    assert_date_function_logic("utc_timestamp");
}

// ---------------------------------------------------------------------------
// Tests from date.rs
// ---------------------------------------------------------------------------

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
    use novarocks::exec::expr::function::date::{compile_tera_format, parse_tera_datetime};
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
    use novarocks::exec::expr::function::date::{compile_tera_format, parse_tera_datetime};
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
    use novarocks::exec::expr::function::date::compile_tera_format;
    let err = compile_tera_format(";YYYYmm:dd").expect_err("format should be invalid");
    assert_eq!(err, "The format parameter ;YYYYmm:dd is invalid format");
}

// ---------------------------------------------------------------------------
// Tests from date_add.rs
// ---------------------------------------------------------------------------

#[test]
fn test_date_add_logic() {
    assert_date_function_logic("date_add");
}

#[test]
fn test_adddate_logic() {
    assert_date_function_logic("adddate");
}

#[test]
fn test_days_add_logic() {
    assert_date_function_logic("days_add");
}

#[test]
fn test_weeks_add_logic() {
    assert_date_function_logic("weeks_add");
}

#[test]
fn test_date_sub_logic() {
    assert_date_function_logic("date_sub");
}

#[test]
fn test_subdate_logic() {
    assert_date_function_logic("subdate");
}

#[test]
fn test_days_sub_logic() {
    assert_date_function_logic("days_sub");
}

#[test]
fn test_weeks_sub_logic() {
    assert_date_function_logic("weeks_sub");
}

// ---------------------------------------------------------------------------
// Tests from date_format.rs
// ---------------------------------------------------------------------------

#[test]
fn test_date_format_logic() {
    assert_date_function_logic("date_format");
}

#[test]
fn test_strftime_logic() {
    assert_date_function_logic("strftime");
}

// ---------------------------------------------------------------------------
// Tests from date_parts.rs
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Tests from date_trunc.rs
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Tests from datediff.rs
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Tests from from_days.rs
// ---------------------------------------------------------------------------

#[test]
fn test_from_days_logic() {
    assert_date_function_logic("from_days");
}

#[test]
fn test_from_days_out_of_calendar_range_returns_zero_date_sentinel() {
    use novarocks::exec::expr::function::date::{FROM_DAYS_MAX_VALID, from_days_value, zero_date_sentinel_date32};
    let sentinel = zero_date_sentinel_date32();
    assert_eq!(from_days_value(-1), Some(sentinel));
    assert_eq!(from_days_value(FROM_DAYS_MAX_VALID + 1), Some(sentinel));
    assert_eq!(from_days_value(i32::MAX as i64), Some(sentinel));
    assert_eq!(from_days_value(i32::MIN as i64), Some(sentinel));
}

#[test]
fn test_from_days_out_of_i32_range_returns_null() {
    use novarocks::exec::expr::function::date::from_days_value;
    assert_eq!(from_days_value(i32::MAX as i64 + 1), None);
    assert_eq!(from_days_value(i32::MIN as i64 - 1), None);
}

// ---------------------------------------------------------------------------
// Tests from from_unixtime.rs
// ---------------------------------------------------------------------------

#[test]
fn test_from_unixtime_logic() {
    assert_date_function_logic("from_unixtime");
}

#[test]
fn test_from_unixtime_ms_logic() {
    assert_date_function_logic("from_unixtime_ms");
}

// ---------------------------------------------------------------------------
// Tests from hour_from_unixtime.rs
// ---------------------------------------------------------------------------

#[test]
fn test_hour_from_unixtime_logic() {
    assert_date_function_logic("hour_from_unixtime");
}

// ---------------------------------------------------------------------------
// Tests from last_day.rs
// ---------------------------------------------------------------------------

#[test]
fn test_last_day_logic() {
    assert_date_function_logic("last_day");
}

// ---------------------------------------------------------------------------
// Tests from makedate.rs
// ---------------------------------------------------------------------------

#[test]
fn test_makedate_logic() {
    assert_date_function_logic("makedate");
}

#[test]
fn test_makedate_bounds_and_year_zero() {
    use novarocks::exec::expr::function::date::eval_makedate;
    let mut arena = ExprArena::default();
    let year_over = arena.push_typed(
        ExprNode::Literal(LiteralValue::Int64(2020)),
        DataType::Int64,
    );
    let day_over =
        arena.push_typed(ExprNode::Literal(LiteralValue::Int64(367)), DataType::Int64);
    let year_zero =
        arena.push_typed(ExprNode::Literal(LiteralValue::Int64(0)), DataType::Int64);
    let day_one = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let year_over_max = arena.push_typed(
        ExprNode::Literal(LiteralValue::Int64(10000)),
        DataType::Int64,
    );
    let year_negative =
        arena.push_typed(ExprNode::Literal(LiteralValue::Int64(-1)), DataType::Int64);

    let expr_date = arena.push_typed(ExprNode::Literal(LiteralValue::Null), DataType::Date32);
    let chunk = common::chunk_len_1();

    let out_over = eval_makedate(&arena, expr_date, &[year_over, day_over], &chunk)
        .expect("makedate eval");
    let out_over = out_over
        .as_any()
        .downcast_ref::<Date32Array>()
        .expect("downcast Date32Array");
    assert!(out_over.is_null(0));

    let out_zero =
        eval_makedate(&arena, expr_date, &[year_zero, day_one], &chunk).expect("makedate eval");
    let out_zero = out_zero
        .as_any()
        .downcast_ref::<Date32Array>()
        .expect("downcast Date32Array");
    assert!(!out_zero.is_null(0));

    let out_over_max = eval_makedate(&arena, expr_date, &[year_over_max, day_one], &chunk)
        .expect("makedate eval");
    let out_over_max = out_over_max
        .as_any()
        .downcast_ref::<Date32Array>()
        .expect("downcast Date32Array");
    assert!(out_over_max.is_null(0));

    let out_negative = eval_makedate(&arena, expr_date, &[year_negative, day_one], &chunk)
        .expect("makedate eval");
    let out_negative = out_negative
        .as_any()
        .downcast_ref::<Date32Array>()
        .expect("downcast Date32Array");
    assert!(out_negative.is_null(0));
}

// ---------------------------------------------------------------------------
// Tests from next_day.rs
// ---------------------------------------------------------------------------

#[test]
fn test_next_day_logic() {
    assert_date_function_logic("next_day");
}

#[test]
fn test_previous_day_logic() {
    assert_date_function_logic("previous_day");
}

// ---------------------------------------------------------------------------
// Tests from sec_to_time.rs
// ---------------------------------------------------------------------------

#[test]
fn test_sec_to_time_logic() {
    assert_date_function_logic("sec_to_time");
}

#[test]
fn test_sec_to_time_boundaries() {
    use novarocks::exec::expr::function::date::format_sec_to_time;
    assert_eq!(format_sec_to_time(-1), "-00:00:01");
    assert_eq!(format_sec_to_time(0), "00:00:00");
    assert_eq!(format_sec_to_time(90061), "25:01:01");
    assert_eq!(format_sec_to_time(3020399), "838:59:59");
    assert_eq!(format_sec_to_time(3020400), "839:00:00");
    assert_eq!(format_sec_to_time(3023999), "839:59:59");
    assert_eq!(format_sec_to_time(3024000), "839:59:59");
}

// ---------------------------------------------------------------------------
// Tests from seconds_add.rs
// ---------------------------------------------------------------------------

#[test]
fn test_seconds_add_logic() {
    assert_date_function_logic("seconds_add");
}

#[test]
fn test_seconds_sub_logic() {
    assert_date_function_logic("seconds_sub");
}

#[test]
fn test_minutes_add_logic() {
    assert_date_function_logic("minutes_add");
}

#[test]
fn test_minutes_sub_logic() {
    assert_date_function_logic("minutes_sub");
}

#[test]
fn test_hours_add_logic() {
    assert_date_function_logic("hours_add");
}

#[test]
fn test_hours_sub_logic() {
    assert_date_function_logic("hours_sub");
}

#[test]
fn test_milliseconds_add_logic() {
    assert_date_function_logic("milliseconds_add");
}

#[test]
fn test_milliseconds_sub_logic() {
    assert_date_function_logic("milliseconds_sub");
}

#[test]
fn test_microseconds_add_logic() {
    assert_date_function_logic("microseconds_add");
}

#[test]
fn test_microseconds_sub_logic() {
    assert_date_function_logic("microseconds_sub");
}

// ---------------------------------------------------------------------------
// Tests from str_to_date.rs
// ---------------------------------------------------------------------------

#[test]
fn test_str_to_date_logic() {
    assert_date_function_logic("str_to_date");
}

#[test]
fn test_str2date_logic() {
    assert_date_function_logic("str2date");
}

// ---------------------------------------------------------------------------
// Tests from substitute.rs
// ---------------------------------------------------------------------------

#[test]
fn test_substitute_logic() {
    assert_date_function_logic("substitute");
}

// ---------------------------------------------------------------------------
// Tests from time_format.rs
// ---------------------------------------------------------------------------

#[test]
fn test_time_format_logic() {
    assert_date_function_logic("time_format");
}

// ---------------------------------------------------------------------------
// Tests from time_slice.rs
// ---------------------------------------------------------------------------

#[test]
fn test_time_slice_logic() {
    assert_date_function_logic("time_slice");
}

#[test]
fn test_date_slice_logic() {
    assert_date_function_logic("date_slice");
}

// ---------------------------------------------------------------------------
// Tests from time_to_sec.rs
// ---------------------------------------------------------------------------

#[test]
fn test_time_to_sec_logic() {
    assert_date_function_logic("time_to_sec");
}

#[test]
fn test_time_to_sec_duration_parser() {
    use novarocks::exec::expr::function::date::parse_hms_duration_to_seconds;
    assert_eq!(parse_hms_duration_to_seconds("00:00:00"), Some(0));
    assert_eq!(parse_hms_duration_to_seconds("23:59:59"), Some(86399));
    assert_eq!(parse_hms_duration_to_seconds("25:00:00"), Some(90000));
    assert_eq!(parse_hms_duration_to_seconds("-00:00:01"), None);
    assert_eq!(parse_hms_duration_to_seconds("0000-00-00 23:59:59"), None);
    assert_eq!(parse_hms_duration_to_seconds("1970-01-01 01:01:01"), None);
}

#[test]
fn test_time_to_sec_recovers_from_failed_cast_time_literal() {
    use novarocks::exec::expr::function::date::{eval_time_to_sec, parse_from_cast_source};
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_i64: ExprId = common::typed_null(&mut arena, DataType::Int64);
    let literal = common::literal_string(&mut arena, "00:00:00");
    let cast_time = arena.push_typed(
        ExprNode::Cast(literal),
        DataType::Timestamp(TimeUnit::Microsecond, None),
    );
    let parsed_from_source =
        parse_from_cast_source(&arena, cast_time, &chunk).expect("parse cast source");
    assert_eq!(parsed_from_source, Some(vec![Some(0)]));
    let out = eval_time_to_sec(&arena, expr_i64, &[cast_time], &chunk)
        .expect("time_to_sec eval");
    let arr = out
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 output");
    assert!(!arr.is_null(0));
    assert_eq!(arr.value(0), 0);
}

#[test]
fn test_time_to_sec_cast_string_with_datetime_prefix_returns_null() {
    use novarocks::exec::expr::function::date::eval_time_to_sec;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_i64: ExprId = common::typed_null(&mut arena, DataType::Int64);
    let literal = common::literal_string(&mut arena, "1970-01-01 01:01:01");
    let cast_time = arena.push_typed(
        ExprNode::Cast(literal),
        DataType::Timestamp(TimeUnit::Microsecond, None),
    );
    let out = eval_time_to_sec(&arena, expr_i64, &[cast_time], &chunk)
        .expect("time_to_sec eval");
    let arr = out
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 output");
    assert!(arr.is_null(0));
}

#[test]
fn test_time_to_sec_explicit_datetime_cast_preserves_time_part() {
    use novarocks::exec::expr::function::date::eval_time_to_sec;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_i64: ExprId = common::typed_null(&mut arena, DataType::Int64);
    let literal = common::literal_string(&mut arena, "1970-01-01 01:01:01");
    let cast_datetime = arena.push_typed(
        ExprNode::Cast(literal),
        DataType::Timestamp(TimeUnit::Microsecond, None),
    );
    let cast_time = arena.push_typed(
        ExprNode::Cast(cast_datetime),
        DataType::Timestamp(TimeUnit::Microsecond, None),
    );
    let out = eval_time_to_sec(&arena, expr_i64, &[cast_time], &chunk)
        .expect("time_to_sec eval");
    let arr = out
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 output");
    assert!(!arr.is_null(0));
    assert_eq!(arr.value(0), 3661);
}

#[test]
fn test_time_to_sec_sec_to_time_negative_roundtrip() {
    use novarocks::exec::expr::function::{FunctionKind};
    use novarocks::exec::expr::function::date::eval_time_to_sec;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_i64: ExprId = common::typed_null(&mut arena, DataType::Int64);
    let seconds = common::literal_i64(&mut arena, -1);
    let sec_to_time = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Date("sec_to_time"),
            args: vec![seconds],
        },
        DataType::Utf8,
    );
    let out = eval_time_to_sec(&arena, expr_i64, &[sec_to_time], &chunk)
        .expect("time_to_sec eval");
    let arr = out
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 output");
    assert!(!arr.is_null(0));
    assert_eq!(arr.value(0), -1);
}

#[test]
fn test_time_to_sec_sec_to_time_negative_clamps_like_sec_to_time() {
    use novarocks::exec::expr::function::{FunctionKind};
    use novarocks::exec::expr::function::date::eval_time_to_sec;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_i64: ExprId = common::typed_null(&mut arena, DataType::Int64);
    let seconds = common::literal_i64(&mut arena, -2_147_483_648);
    let sec_to_time = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Date("sec_to_time"),
            args: vec![seconds],
        },
        DataType::Utf8,
    );
    let out = eval_time_to_sec(&arena, expr_i64, &[sec_to_time], &chunk)
        .expect("time_to_sec eval");
    let arr = out
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 output");
    assert!(!arr.is_null(0));
    assert_eq!(arr.value(0), -3_023_999);
}

// ---------------------------------------------------------------------------
// Tests from timediff.rs
// ---------------------------------------------------------------------------

#[test]
fn test_timediff_logic() {
    assert_date_function_logic("timediff");
}

// ---------------------------------------------------------------------------
// Tests from timestampadd.rs
// ---------------------------------------------------------------------------

#[test]
fn test_timestampadd_logic() {
    assert_date_function_logic("timestampadd");
}

// ---------------------------------------------------------------------------
// Tests from timestampdiff.rs
// ---------------------------------------------------------------------------

#[test]
fn test_timestampdiff_logic() {
    assert_date_function_logic("timestampdiff");
}

// ---------------------------------------------------------------------------
// Tests from to_days.rs
// ---------------------------------------------------------------------------

#[test]
fn test_to_days_logic() {
    assert_date_function_logic("to_days");
}

// ---------------------------------------------------------------------------
// Tests from to_iso8601.rs
// ---------------------------------------------------------------------------

#[test]
fn test_to_iso8601_logic() {
    assert_date_function_logic("to_iso8601");
}

// ---------------------------------------------------------------------------
// Tests from unix_timestamp.rs
// ---------------------------------------------------------------------------

#[test]
fn test_unix_timestamp_logic() {
    assert_date_function_logic("unix_timestamp");
}

// ---------------------------------------------------------------------------
// Tests from common.rs
// ---------------------------------------------------------------------------

#[test]
fn test_parse_datetime_rejects_second_60() {
    use novarocks::exec::expr::function::date::parse_datetime;
    // second=60 is a chrono leap-second; StarRocks treats it as invalid → None.
    assert!(parse_datetime("2024-01-01 01:30:60").is_none());
    assert!(parse_datetime("2024-01-01T01:30:60").is_none());
    // second=61 and beyond must also be rejected
    assert!(parse_datetime("2024-01-01 01:30:61").is_none());
    // Valid boundary: second=59 is accepted
    assert!(parse_datetime("2024-01-01 01:30:59").is_some());
}

#[test]
fn parse_datetime_accepts_lenient_second_field_width() {
    use novarocks::exec::expr::function::date::parse_datetime;
    let dt = parse_datetime("2023-08-17 08:00:006").expect("parse datetime");
    assert_eq!(
        dt,
        NaiveDate::from_ymd_opt(2023, 8, 17)
            .unwrap()
            .and_hms_opt(8, 0, 6)
            .unwrap()
    );
}

#[test]
fn parse_datetime_accepts_compact_timestamp() {
    use novarocks::exec::expr::function::date::parse_datetime;
    let dt = parse_datetime("20230817T080006").expect("parse compact datetime");
    assert_eq!(
        dt,
        NaiveDate::from_ymd_opt(2023, 8, 17)
            .unwrap()
            .and_hms_opt(8, 0, 6)
            .unwrap()
    );
}

// ---------------------------------------------------------------------------
// Tests from dispatch.rs
// ---------------------------------------------------------------------------

#[test]
fn test_date_parts_and_names() {
    let mut arena = ExprArena::default();
    let dt = common::literal_string(&mut arena, "2020-01-02 03:04:05");
    let chunk = common::chunk_len_1();
    let expr_i64 = common::typed_null(&mut arena, DataType::Int64);
    let expr_str = common::typed_null(&mut arena, DataType::Utf8);

    assert_eq!(date_eval_i64("day", &arena, expr_i64, &[dt], &chunk), 2);
    assert_eq!(date_eval_i64("dayofmonth", &arena, expr_i64, &[dt], &chunk), 2);
    assert_eq!(date_eval_i64("dayofweek", &arena, expr_i64, &[dt], &chunk), 5);
    assert_eq!(
        date_eval_i64("dayofweek_iso", &arena, expr_i64, &[dt], &chunk),
        4
    );
    assert_eq!(date_eval_i64("dayofyear", &arena, expr_i64, &[dt], &chunk), 2);
    assert_eq!(date_eval_i64("month", &arena, expr_i64, &[dt], &chunk), 1);
    assert_eq!(date_eval_i64("year", &arena, expr_i64, &[dt], &chunk), 2020);
    assert_eq!(date_eval_i64("hour", &arena, expr_i64, &[dt], &chunk), 3);
    assert_eq!(date_eval_i64("minute", &arena, expr_i64, &[dt], &chunk), 4);
    assert_eq!(date_eval_i64("second", &arena, expr_i64, &[dt], &chunk), 5);
    assert_eq!(date_eval_i64("quarter", &arena, expr_i64, &[dt], &chunk), 1);
    assert_eq!(date_eval_i64("week", &arena, expr_i64, &[dt], &chunk), 1);
    assert_eq!(date_eval_i64("week_iso", &arena, expr_i64, &[dt], &chunk), 1);
    assert_eq!(date_eval_i64("weekday", &arena, expr_i64, &[dt], &chunk), 3);
    assert_eq!(date_eval_i64("weekofyear", &arena, expr_i64, &[dt], &chunk), 1);
    assert_eq!(
        date_eval_str("dayname", &arena, expr_str, &[dt], &chunk),
        "Thursday"
    );
    assert_eq!(
        date_eval_str("monthname", &arena, expr_str, &[dt], &chunk),
        "January"
    );
}

#[test]
fn test_date_add_sub_variants() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_ts = common::typed_null(&mut arena, DataType::Timestamp(TimeUnit::Microsecond, None));

    let base_add = common::literal_string(&mut arena, "2020-01-01 00:00:00");
    let base_sub = common::literal_string(&mut arena, "2020-01-03 00:00:00");
    let days = common::literal_i64(&mut arena, 2);

    let expected_add = dt_micros("2020-01-03 00:00:00");
    let expected_sub = dt_micros("2020-01-01 00:00:00");
    assert_eq!(
        date_eval_ts("date_add", &arena, expr_ts, &[base_add, days], &chunk),
        expected_add
    );
    assert_eq!(
        date_eval_ts("adddate", &arena, expr_ts, &[base_add, days], &chunk),
        expected_add
    );
    assert_eq!(
        date_eval_ts("days_add", &arena, expr_ts, &[base_add, days], &chunk),
        expected_add
    );
    assert_eq!(
        date_eval_ts("date_sub", &arena, expr_ts, &[base_sub, days], &chunk),
        expected_sub
    );
    assert_eq!(
        date_eval_ts("subdate", &arena, expr_ts, &[base_sub, days], &chunk),
        expected_sub
    );
    assert_eq!(
        date_eval_ts("days_sub", &arena, expr_ts, &[base_sub, days], &chunk),
        expected_sub
    );

    let base_week = common::literal_string(&mut arena, "2020-01-01 00:00:00");
    let weeks = common::literal_i64(&mut arena, 1);
    assert_eq!(
        date_eval_ts("weeks_add", &arena, expr_ts, &[base_week, weeks], &chunk),
        dt_micros("2020-01-08 00:00:00")
    );
    let base_week_sub = common::literal_string(&mut arena, "2020-01-08 00:00:00");
    assert_eq!(
        date_eval_ts(
            "weeks_sub",
            &arena,
            expr_ts,
            &[base_week_sub, weeks],
            &chunk
        ),
        dt_micros("2020-01-01 00:00:00")
    );

    let base_hour = common::literal_string(&mut arena, "2020-01-01 01:00:00");
    let hours = common::literal_i64(&mut arena, 2);
    assert_eq!(
        date_eval_ts("hours_add", &arena, expr_ts, &[base_hour, hours], &chunk),
        dt_micros("2020-01-01 03:00:00")
    );
    let base_hour_sub = common::literal_string(&mut arena, "2020-01-01 03:00:00");
    assert_eq!(
        date_eval_ts(
            "hours_sub",
            &arena,
            expr_ts,
            &[base_hour_sub, hours],
            &chunk
        ),
        dt_micros("2020-01-01 01:00:00")
    );

    let base_min = common::literal_string(&mut arena, "2020-01-01 00:01:00");
    let mins = common::literal_i64(&mut arena, 1);
    assert_eq!(
        date_eval_ts("minutes_add", &arena, expr_ts, &[base_min, mins], &chunk),
        dt_micros("2020-01-01 00:02:00")
    );
    let base_min_sub = common::literal_string(&mut arena, "2020-01-01 00:02:00");
    assert_eq!(
        date_eval_ts(
            "minutes_sub",
            &arena,
            expr_ts,
            &[base_min_sub, mins],
            &chunk
        ),
        dt_micros("2020-01-01 00:01:00")
    );

    let base_sec = common::literal_string(&mut arena, "2020-01-01 00:00:01");
    let secs = common::literal_i64(&mut arena, 2);
    assert_eq!(
        date_eval_ts("seconds_add", &arena, expr_ts, &[base_sec, secs], &chunk),
        dt_micros("2020-01-01 00:00:03")
    );
    let base_sec_sub = common::literal_string(&mut arena, "2020-01-01 00:00:03");
    assert_eq!(
        date_eval_ts(
            "seconds_sub",
            &arena,
            expr_ts,
            &[base_sec_sub, secs],
            &chunk
        ),
        dt_micros("2020-01-01 00:00:01")
    );

    let base_ms = common::literal_string(&mut arena, "2020-01-01 00:00:00");
    let millis = common::literal_i64(&mut arena, 1500);
    assert_eq!(
        date_eval_ts(
            "milliseconds_add",
            &arena,
            expr_ts,
            &[base_ms, millis],
            &chunk
        ),
        dt_micros("2020-01-01 00:00:01") + 500_000
    );
    let base_ms_sub = common::literal_string(&mut arena, "2020-01-01 00:00:02");
    assert_eq!(
        date_eval_ts(
            "milliseconds_sub",
            &arena,
            expr_ts,
            &[base_ms_sub, millis],
            &chunk
        ),
        dt_micros("2020-01-01 00:00:00") + 500_000
    );

    let micros = common::literal_i64(&mut arena, 1000);
    assert_eq!(
        date_eval_ts(
            "microseconds_add",
            &arena,
            expr_ts,
            &[base_ms, micros],
            &chunk
        ),
        dt_micros("2020-01-01 00:00:00") + 1000
    );
    let base_micro_sub = common::literal_string(&mut arena, "2020-01-01 00:00:01");
    assert_eq!(
        date_eval_ts(
            "microseconds_sub",
            &arena,
            expr_ts,
            &[base_micro_sub, micros],
            &chunk
        ),
        dt_micros("2020-01-01 00:00:01") - 1000
    );

    let invalid_date = common::literal_string(&mut arena, "abcd");
    let one_day = common::literal_i64(&mut arena, 1);
    let arr = eval_date_function(
        "days_add",
        &arena,
        expr_ts,
        &[invalid_date, one_day],
        &chunk,
    )
    .expect("days_add should return NULL for invalid datetime input");
    let arr = arr
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("TimestampMicrosecondArray");
    assert!(arr.is_null(0));

    let valid_date = common::literal_string(&mut arena, "2024-01-01");
    let invalid_days = common::literal_string(&mut arena, "x");
    let arr = eval_date_function(
        "days_add",
        &arena,
        expr_ts,
        &[valid_date, invalid_days],
        &chunk,
    )
    .expect("days_add should return NULL for non-numeric day delta");
    let arr = arr
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("TimestampMicrosecondArray");
    assert!(arr.is_null(0));
}

#[test]
fn test_add_months_and_variants() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_ts = common::typed_null(&mut arena, DataType::Timestamp(TimeUnit::Microsecond, None));
    let base = common::literal_string(&mut arena, "2020-01-15 00:00:00");
    let months = common::literal_i64(&mut arena, 2);
    assert_eq!(
        date_eval_ts("add_months", &arena, expr_ts, &[base, months], &chunk),
        dt_micros("2020-03-15 00:00:00")
    );
    assert_eq!(
        date_eval_ts("months_add", &arena, expr_ts, &[base, months], &chunk),
        dt_micros("2020-03-15 00:00:00")
    );

    let base_sub = common::literal_string(&mut arena, "2020-03-15 00:00:00");
    let one = common::literal_i64(&mut arena, 1);
    assert_eq!(
        date_eval_ts("months_sub", &arena, expr_ts, &[base_sub, one], &chunk),
        dt_micros("2020-02-15 00:00:00")
    );

    let years = common::literal_i64(&mut arena, 1);
    assert_eq!(
        date_eval_ts("years_add", &arena, expr_ts, &[base, years], &chunk),
        dt_micros("2021-01-15 00:00:00")
    );
    let base_year_sub = common::literal_string(&mut arena, "2021-01-15 00:00:00");
    assert_eq!(
        date_eval_ts(
            "years_sub",
            &arena,
            expr_ts,
            &[base_year_sub, years],
            &chunk
        ),
        dt_micros("2020-01-15 00:00:00")
    );

    let quarters = common::literal_i64(&mut arena, 1);
    assert_eq!(
        date_eval_ts("quarters_add", &arena, expr_ts, &[base, quarters], &chunk),
        dt_micros("2020-04-15 00:00:00")
    );
    let base_quarter_sub = common::literal_string(&mut arena, "2020-04-15 00:00:00");
    assert_eq!(
        date_eval_ts(
            "quarters_sub",
            &arena,
            expr_ts,
            &[base_quarter_sub, quarters],
            &chunk
        ),
        dt_micros("2020-01-15 00:00:00")
    );

    let invalid_base = common::literal_string(&mut arena, "abcd");
    let one_month = common::literal_i64(&mut arena, 1);
    let arr = eval_date_function(
        "months_add",
        &arena,
        expr_ts,
        &[invalid_base, one_month],
        &chunk,
    )
    .expect("months_add should return NULL for invalid datetime input");
    let arr = arr
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("TimestampMicrosecondArray");
    assert!(arr.is_null(0));

    let valid_base = common::literal_string(&mut arena, "2024-01-01");
    let invalid_month = common::literal_string(&mut arena, "x");
    let arr = eval_date_function(
        "months_add",
        &arena,
        expr_ts,
        &[valid_base, invalid_month],
        &chunk,
    )
    .expect("months_add should return NULL for non-numeric month delta");
    let arr = arr
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("TimestampMicrosecondArray");
    assert!(arr.is_null(0));
}

#[test]
fn test_diff_functions() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_i64 = common::typed_null(&mut arena, DataType::Int64);

    let d1 = common::literal_string(&mut arena, "2020-01-03 00:00:00");
    let d2 = common::literal_string(&mut arena, "2020-01-01 00:00:00");
    assert_eq!(date_eval_i64("datediff", &arena, expr_i64, &[d1, d2], &chunk), 2);
    assert_eq!(
        date_eval_i64("days_diff", &arena, expr_i64, &[d1, d2], &chunk),
        2
    );

    let w1 = common::literal_string(&mut arena, "2020-01-15 00:00:00");
    let w2 = common::literal_string(&mut arena, "2020-01-01 00:00:00");
    assert_eq!(
        date_eval_i64("weeks_diff", &arena, expr_i64, &[w1, w2], &chunk),
        2
    );

    let m1 = common::literal_string(&mut arena, "2020-03-15 00:00:00");
    let m2 = common::literal_string(&mut arena, "2020-01-15 00:00:00");
    assert_eq!(
        date_eval_i64("months_diff", &arena, expr_i64, &[m1, m2], &chunk),
        2
    );
    let m3 = common::literal_string(&mut arena, "2024-01-31 00:00:00");
    let m4 = common::literal_string(&mut arena, "2024-02-01 00:00:00");
    assert_eq!(
        date_eval_i64("months_diff", &arena, expr_i64, &[m3, m4], &chunk),
        0
    );
    let m5 = common::literal_string(&mut arena, "2024-02-01 00:00:00");
    let m6 = common::literal_string(&mut arena, "2024-01-31 00:00:00");
    assert_eq!(
        date_eval_i64("months_diff", &arena, expr_i64, &[m5, m6], &chunk),
        0
    );

    let y1 = common::literal_string(&mut arena, "2021-01-15 00:00:00");
    let y2 = common::literal_string(&mut arena, "2020-01-15 00:00:00");
    assert_eq!(
        date_eval_i64("years_diff", &arena, expr_i64, &[y1, y2], &chunk),
        1
    );
    let leap_l = common::literal_string(&mut arena, "2026-02-28");
    let leap_r = common::literal_string(&mut arena, "2024-02-29");
    assert_eq!(
        date_eval_i64("years_diff", &arena, expr_i64, &[leap_l, leap_r], &chunk),
        1
    );

    let h1 = common::literal_string(&mut arena, "2020-01-01 03:00:00");
    let h2 = common::literal_string(&mut arena, "2020-01-01 01:00:00");
    assert_eq!(
        date_eval_i64("hours_diff", &arena, expr_i64, &[h1, h2], &chunk),
        2
    );

    let n1 = common::literal_string(&mut arena, "2020-01-01 00:02:00");
    let n2 = common::literal_string(&mut arena, "2020-01-01 00:01:00");
    assert_eq!(
        date_eval_i64("minutes_diff", &arena, expr_i64, &[n1, n2], &chunk),
        1
    );

    let s1 = common::literal_string(&mut arena, "2020-01-01 00:00:03");
    let s2 = common::literal_string(&mut arena, "2020-01-01 00:00:01");
    assert_eq!(
        date_eval_i64("seconds_diff", &arena, expr_i64, &[s1, s2], &chunk),
        2
    );

    let q1 = common::literal_string(&mut arena, "2020-05-01 00:00:00");
    let q2 = common::literal_string(&mut arena, "2020-02-01 00:00:00");
    assert_eq!(
        date_eval_i64("quarters_diff", &arena, expr_i64, &[q1, q2], &chunk),
        1
    );
}

#[test]
fn test_date_and_datetime_conversions() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_date = common::typed_null(&mut arena, DataType::Date32);
    let expr_ts = common::typed_null(&mut arena, DataType::Timestamp(TimeUnit::Microsecond, None));

    let dt = common::literal_string(&mut arena, "2020-01-02 03:04:05");
    let expected_date = date32_from_ymd(2020, 1, 2);
    assert_eq!(
        date_eval_date32("date", &arena, expr_date, &[dt], &chunk),
        expected_date
    );
    assert_eq!(
        date_eval_date32("to_date", &arena, expr_date, &[dt], &chunk),
        expected_date
    );
    assert_eq!(
        date_eval_date32("to_tera_date", &arena, expr_date, &[dt], &chunk),
        expected_date
    );

    let expected_ts = dt_micros("2020-01-02 03:04:05");
    assert_eq!(
        date_eval_ts("timestamp", &arena, expr_ts, &[dt], &chunk),
        expected_ts
    );
    assert_eq!(
        date_eval_ts("to_datetime", &arena, expr_ts, &[dt], &chunk),
        expected_ts
    );
}

#[test]
fn test_format_and_parse() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_str = common::typed_null(&mut arena, DataType::Utf8);
    let expr_date = common::typed_null(&mut arena, DataType::Date32);
    let dt = common::literal_string(&mut arena, "2020-01-02 03:04:05");
    let tm = common::literal_string(&mut arena, "12:34:56");
    let fmt_date = common::literal_string(&mut arena, "%Y-%m-%d");
    let fmt_time = common::literal_string(&mut arena, "%H:%i:%s");

    assert_eq!(
        date_eval_str("date_format", &arena, expr_str, &[dt, fmt_date], &chunk),
        "2020-01-02"
    );
    assert_eq!(
        date_eval_str("strftime", &arena, expr_str, &[dt, fmt_date], &chunk),
        "2020-01-02"
    );
    assert_eq!(
        date_eval_str("time_format", &arena, expr_str, &[tm, fmt_time], &chunk),
        "00:00:00"
    );
    let arr = eval_date_function("time_format", &arena, expr_str, &[dt, fmt_time], &chunk)
        .expect("time_format should evaluate");
    let arr = arr
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("StringArray");
    assert!(arr.is_null(0));

    let s = common::literal_string(&mut arena, "2020-01-02");
    let fmt = common::literal_string(&mut arena, "%Y-%m-%d");
    let expected_date = date32_from_ymd(2020, 1, 2);
    assert_eq!(
        date_eval_date32("str_to_date", &arena, expr_date, &[s, fmt], &chunk),
        expected_date
    );
    assert_eq!(
        date_eval_date32("str2date", &arena, expr_date, &[s, fmt], &chunk),
        expected_date
    );
}

#[test]
fn test_timestampadd_and_timestampdiff() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_ts = common::typed_null(&mut arena, DataType::Timestamp(TimeUnit::Microsecond, None));
    let expr_i64 = common::typed_null(&mut arena, DataType::Int64);

    let unit = common::literal_string(&mut arena, "day");
    let interval = common::literal_i64(&mut arena, 2);
    let dt = common::literal_string(&mut arena, "2020-01-01 00:00:00");
    assert_eq!(
        date_eval_ts(
            "timestampadd",
            &arena,
            expr_ts,
            &[unit, interval, dt],
            &chunk
        ),
        dt_micros("2020-01-03 00:00:00")
    );

    let unit_h = common::literal_string(&mut arena, "hour");
    let dt1 = common::literal_string(&mut arena, "2020-01-01 00:00:00");
    let dt2 = common::literal_string(&mut arena, "2020-01-01 03:00:00");
    assert_eq!(
        date_eval_i64(
            "timestampdiff",
            &arena,
            expr_i64,
            &[unit_h, dt1, dt2],
            &chunk
        ),
        3
    );

    let unit_ms = common::literal_string(&mut arena, "millisecond");
    let dt_ms1 = common::literal_string(&mut arena, "2020-01-01 00:00:00");
    let dt_ms2 = common::literal_string(&mut arena, "2020-01-01 00:00:00.250000");
    assert_eq!(
        date_eval_i64(
            "timestampdiff",
            &arena,
            expr_i64,
            &[unit_ms, dt_ms1, dt_ms2],
            &chunk
        ),
        250
    );

    let left = common::literal_string(&mut arena, "2020-01-01 00:00:03");
    let right = common::literal_string(&mut arena, "2020-01-01 00:00:01");
    assert_eq!(
        date_eval_i64(
            "milliseconds_diff",
            &arena,
            expr_i64,
            &[left, right],
            &chunk
        ),
        2000
    );
}

#[test]
fn test_convert_tz_dispatch() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_ts = common::typed_null(&mut arena, DataType::Timestamp(TimeUnit::Microsecond, None));
    let dt = common::literal_string(&mut arena, "2020-01-01 00:00:00");
    let from = common::literal_string(&mut arena, "+00:00");
    let to = common::literal_string(&mut arena, "+08:00");
    assert_eq!(
        date_eval_ts("convert_tz", &arena, expr_ts, &[dt, from, to], &chunk),
        dt_micros("2020-01-01 08:00:00")
    );
}

#[test]
fn test_unix_timestamp_and_from_unixtime() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_ts = common::typed_null(&mut arena, DataType::Timestamp(TimeUnit::Microsecond, None));
    let expr_i64 = common::typed_null(&mut arena, DataType::Int64);
    let expr_str = common::typed_null(&mut arena, DataType::Utf8);

    let secs_one = common::literal_i64(&mut arena, 1);
    let expected_local = Local
        .timestamp_opt(1, 0)
        .single()
        .map(|dt| dt.naive_local().and_utc().timestamp_micros())
        .unwrap();
    assert_eq!(
        date_eval_ts("from_unixtime", &arena, expr_ts, &[secs_one], &chunk),
        expected_local
    );
    let millis = common::literal_i64(&mut arena, 1_000);
    assert_eq!(
        date_eval_ts("from_unixtime_ms", &arena, expr_ts, &[millis], &chunk),
        expected_local
    );
    let secs_two = common::literal_i64(&mut arena, 1);
    let fmt_date = common::literal_string(&mut arena, "%Y-%m-%d");
    assert_eq!(
        date_eval_str(
            "from_unixtime",
            &arena,
            expr_str,
            &[secs_two, fmt_date],
            &chunk
        ),
        "1970-01-01"
    );
    let secs_three = common::literal_i64(&mut arena, 1);
    let fmt_dt = common::literal_string(&mut arena, "yyyy-MM-dd HH:mm:ss");
    let tz = common::literal_string(&mut arena, "UTC");
    assert_eq!(
        date_eval_str(
            "from_unixtime",
            &arena,
            expr_str,
            &[secs_three, fmt_dt, tz],
            &chunk
        ),
        "1970-01-01 00:00:01"
    );
    arena.set_session_time_zone(Some("+10:00".to_string()));
    let secs_four = common::literal_i64(&mut arena, 1_196_440_219);
    let fmt_session = common::literal_string(&mut arena, "yyyy-MM-dd HH:mm:ss");
    assert_eq!(
        date_eval_str(
            "from_unixtime",
            &arena,
            expr_str,
            &[secs_four, fmt_session],
            &chunk
        ),
        "2007-12-01 02:30:19"
    );

    let dt = common::literal_string(&mut arena, "1970-01-01 00:00:01");
    assert_eq!(
        date_eval_i64("unix_timestamp", &arena, expr_i64, &[dt], &chunk),
        1
    );

    let secs = common::literal_i64(&mut arena, 5 * 3600);
    let expected_hour = Local
        .timestamp_opt(5 * 3600, 0)
        .single()
        .map(|dt| dt.hour() as i64)
        .unwrap();
    assert_eq!(
        date_eval_i64("hour_from_unixtime", &arena, expr_i64, &[secs], &chunk),
        expected_hour
    );
}

#[test]
fn test_to_days_and_from_days() {
    use novarocks::exec::expr::function::date::common::{BC_EPOCH_JULIAN, julian_from_date};
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_i64 = common::typed_null(&mut arena, DataType::Int64);
    let expr_date = common::typed_null(&mut arena, DataType::Date32);

    let date = NaiveDate::from_ymd_opt(1970, 1, 2).unwrap();
    let days = (julian_from_date(date) - BC_EPOCH_JULIAN) as i64;
    let date_str = common::literal_string(&mut arena, "1970-01-02");
    assert_eq!(
        date_eval_i64("to_days", &arena, expr_i64, &[date_str], &chunk),
        days
    );

    let days_lit = common::literal_i64(&mut arena, days);
    let expected_date = date32_from_ymd(1970, 1, 2);
    assert_eq!(
        date_eval_date32("from_days", &arena, expr_date, &[days_lit], &chunk),
        expected_date
    );
}

#[test]
fn test_last_day_makedate_next_previous() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_date = common::typed_null(&mut arena, DataType::Date32);

    let d = common::literal_string(&mut arena, "2020-02-10");
    assert_eq!(
        date_eval_date32("last_day", &arena, expr_date, &[d], &chunk),
        date32_from_ymd(2020, 2, 29)
    );
    let d_quarter = common::literal_string(&mut arena, "2020-02-10");
    let quarter = common::literal_string(&mut arena, "quarter");
    assert_eq!(
        date_eval_date32("last_day", &arena, expr_date, &[d_quarter, quarter], &chunk),
        date32_from_ymd(2020, 3, 31)
    );
    let d_year = common::literal_string(&mut arena, "2020-02-10");
    let year_unit = common::literal_string(&mut arena, "year");
    assert_eq!(
        date_eval_date32("last_day", &arena, expr_date, &[d_year, year_unit], &chunk),
        date32_from_ymd(2020, 12, 31)
    );

    let year = common::literal_i64(&mut arena, 2020);
    let day = common::literal_i64(&mut arena, 32);
    assert_eq!(
        date_eval_date32("makedate", &arena, expr_date, &[year, day], &chunk),
        date32_from_ymd(2020, 2, 1)
    );

    let base = common::literal_string(&mut arena, "2020-01-02");
    let friday = common::literal_string(&mut arena, "Fr");
    assert_eq!(
        date_eval_date32("next_day", &arena, expr_date, &[base, friday], &chunk),
        date32_from_ymd(2020, 1, 3)
    );
    let invalid = common::literal_string(&mut arena, "mon");
    let next_err = eval_date_function("next_day", &arena, expr_date, &[base, invalid], &chunk)
        .unwrap_err();
    assert!(next_err.contains("mon not supported in next_day dow_string backend"));
    let prev = common::literal_string(&mut arena, "2020-01-02");
    let friday2 = common::literal_string(&mut arena, "Fr");
    assert_eq!(
        date_eval_date32("previous_day", &arena, expr_date, &[prev, friday2], &chunk),
        date32_from_ymd(2019, 12, 27)
    );
    let monday2 = common::literal_string(&mut arena, "mon");
    let prev_err =
        eval_date_function("previous_day", &arena, expr_date, &[prev, monday2], &chunk)
            .unwrap_err();
    assert!(prev_err.contains("mon not supported in previous_day dow_string backend"));
}

#[test]
fn test_numeric_coercion_for_makedate_and_microseconds() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_date = common::typed_null(&mut arena, DataType::Date32);
    let expr_ts = common::typed_null(&mut arena, DataType::Timestamp(TimeUnit::Microsecond, None));

    let year_str = common::literal_string(&mut arena, "2024");
    let day_float = common::literal_f64(&mut arena, 60.9);
    assert_eq!(
        date_eval_date32(
            "makedate",
            &arena,
            expr_date,
            &[year_str, day_float],
            &chunk
        ),
        date32_from_ymd(2024, 2, 29)
    );

    let base_dt = common::literal_string(&mut arena, "2024-02-29 12:00:00");
    let micros_str = common::literal_string(&mut arena, "123456");
    assert_eq!(
        date_eval_ts(
            "microseconds_add",
            &arena,
            expr_ts,
            &[base_dt, micros_str],
            &chunk
        ),
        dt_micros("2024-02-29 12:00:00") + 123_456
    );
}

#[test]
fn test_hour_from_unixtime_out_of_range_returns_null() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_i64 = common::typed_null(&mut arena, DataType::Int64);

    let neg = common::literal_i64(&mut arena, -1);
    let arr =
        eval_date_function("hour_from_unixtime", &arena, expr_i64, &[neg], &chunk).unwrap();
    let arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(arr.is_null(0));

    let overflow = common::literal_i64(&mut arena, 253_402_243_200);
    let arr = eval_date_function("hour_from_unixtime", &arena, expr_i64, &[overflow], &chunk)
        .unwrap();
    let arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(arr.is_null(0));
}

#[test]
fn test_next_previous_day_invalid_dow_string() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_date = common::typed_null(&mut arena, DataType::Date32);

    let base = common::literal_string(&mut arena, "2020-01-02");
    let invalid = common::literal_string(&mut arena, "mon");
    let err = eval_date_function("next_day", &arena, expr_date, &[base, invalid], &chunk)
        .expect_err("next_day should reject lowercase dow string");
    assert!(err.contains("mon not supported in next_day dow_string backend"));

    let base_prev = common::literal_string(&mut arena, "2020-01-02");
    let invalid_prev = common::literal_string(&mut arena, "monday");
    let err_prev = eval_date_function(
        "previous_day",
        &arena,
        expr_date,
        &[base_prev, invalid_prev],
        &chunk,
    )
    .expect_err("previous_day should reject lowercase dow string");
    assert!(err_prev.contains("monday not supported in previous_day dow_string backend"));
}

#[test]
fn test_next_previous_day_null_short_circuit() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_date = common::typed_null(&mut arena, DataType::Date32);

    let null_date = common::typed_null(&mut arena, DataType::Utf8);
    let invalid = common::literal_string(&mut arena, "xxx");
    let arr = eval_date_function("next_day", &arena, expr_date, &[null_date, invalid], &chunk)
        .expect("null date should short-circuit to NULL without validating day token");
    let arr = arr.as_any().downcast_ref::<Date32Array>().unwrap();
    assert!(arr.is_null(0));
}

#[test]
fn test_time_to_sec_and_sec_to_time_and_timediff() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_i64 = common::typed_null(&mut arena, DataType::Int64);
    let expr_str = common::typed_null(&mut arena, DataType::Utf8);

    let dt = common::literal_string(&mut arena, "01:01:01");
    assert_eq!(
        date_eval_i64("time_to_sec", &arena, expr_i64, &[dt], &chunk),
        3661
    );

    let secs = common::literal_i64(&mut arena, 3661);
    assert_eq!(
        date_eval_str("sec_to_time", &arena, expr_str, &[secs], &chunk),
        "01:01:01"
    );

    let t1 = common::literal_string(&mut arena, "2020-01-02 03:00:00");
    let t2 = common::literal_string(&mut arena, "2020-01-02 01:00:00");
    assert_eq!(
        date_eval_str("timediff", &arena, expr_str, &[t1, t2], &chunk),
        "02:00:00"
    );
}

#[test]
fn test_trunc_and_slice_and_alignment() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_ts = common::typed_null(&mut arena, DataType::Timestamp(TimeUnit::Microsecond, None));
    let expr_date = common::typed_null(&mut arena, DataType::Date32);
    let unit_month = common::literal_string(&mut arena, "month");
    let dt = common::literal_string(&mut arena, "2020-01-15 12:34:56");
    assert_eq!(
        date_eval_ts("date_trunc", &arena, expr_ts, &[unit_month, dt], &chunk),
        dt_micros("2020-01-01 00:00:00")
    );
    assert_eq!(
        date_eval_date32("date_trunc", &arena, expr_date, &[unit_month, dt], &chunk),
        date32_from_ymd(2020, 1, 1)
    );
    let unit_millisecond = common::literal_string(&mut arena, "millisecond");
    let dt_ms = common::literal_string(&mut arena, "2020-01-15 12:34:56.123456");
    let expected_ms =
        NaiveDateTime::parse_from_str("2020-01-15 12:34:56.123000", "%Y-%m-%d %H:%M:%S%.f")
            .unwrap()
            .and_utc()
            .timestamp_micros();
    assert_eq!(
        date_eval_ts(
            "date_trunc",
            &arena,
            expr_ts,
            &[unit_millisecond, dt_ms],
            &chunk
        ),
        expected_ms
    );
    let invalid_unit = common::literal_string(&mut arena, "foo");
    let invalid_dt = common::literal_string(&mut arena, "2020-01-15 12:34:56");
    let err = eval_date_function(
        "date_trunc",
        &arena,
        expr_ts,
        &[invalid_unit, invalid_dt],
        &chunk,
    )
    .expect_err("invalid date_trunc unit should return error");
    assert!(err.contains("format value must in"));
    assert_eq!(
        date_eval_ts("date_floor", &arena, expr_ts, &[unit_month, dt], &chunk),
        dt_micros("2020-01-01 00:00:00")
    );

    let unit_day = common::literal_string(&mut arena, "day");
    assert_eq!(
        date_eval_ts(
            "alignment_timestamp",
            &arena,
            expr_ts,
            &[unit_day, dt],
            &chunk
        ),
        dt_micros("2020-01-15 00:00:00")
    );

    let interval = common::literal_i64(&mut arena, 1);
    let unit_hour = common::literal_string(&mut arena, "hour");
    let dt2 = common::literal_string(&mut arena, "2020-01-02 03:04:05");
    assert_eq!(
        date_eval_ts(
            "time_slice",
            &arena,
            expr_ts,
            &[dt2, interval, unit_hour],
            &chunk
        ),
        dt_micros("2020-01-02 03:00:00")
    );
    let dt3 = common::literal_string(&mut arena, "2020-01-02 03:04:05");
    assert_eq!(
        date_eval_ts(
            "date_slice",
            &arena,
            expr_ts,
            &[dt3, interval, unit_hour],
            &chunk
        ),
        dt_micros("2020-01-02 03:00:00")
    );
}

#[test]
fn test_substitute_identity() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_str = common::typed_null(&mut arena, DataType::Utf8);
    let s = common::literal_string(&mut arena, "2020-01-02");
    assert_eq!(
        date_eval_str("substitute", &arena, expr_str, &[s], &chunk),
        "2020-01-02"
    );
}

#[test]
fn test_current_and_utc() {
    use chrono::Utc;
    use novarocks::exec::expr::function::date::common::naive_to_date32;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_ts = common::typed_null(&mut arena, DataType::Timestamp(TimeUnit::Microsecond, None));
    let expr_date = common::typed_null(&mut arena, DataType::Date32);

    let now_local = Local::now().naive_local();
    let now_local_ts = now_local.and_utc().timestamp_micros();
    let actual = date_eval_ts("current_timestamp", &arena, expr_ts, &[], &chunk);
    assert!((actual - now_local_ts).abs() < 5_000_000);

    let today = now_local.date();
    let actual_date = date_eval_date32("current_date", &arena, expr_date, &[], &chunk);
    assert_eq!(actual_date, naive_to_date32(today));
    let actual_curdate = date_eval_date32("curdate", &arena, expr_date, &[], &chunk);
    assert_eq!(actual_curdate, naive_to_date32(today));

    let now_time = now_local.time();
    let base = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let expected_time_ts = base.and_time(now_time).and_utc().timestamp_micros();
    let actual_time = date_eval_ts("current_time", &arena, expr_ts, &[], &chunk);
    assert!((actual_time - expected_time_ts).abs() < 5_000_000);
    let actual_curtime = date_eval_ts("curtime", &arena, expr_ts, &[], &chunk);
    assert!((actual_curtime - expected_time_ts).abs() < 5_000_000);

    let actual_now = date_eval_ts("now", &arena, expr_ts, &[], &chunk);
    assert!((actual_now - now_local_ts).abs() < 5_000_000);
    let actual_localtime = date_eval_ts("localtime", &arena, expr_ts, &[], &chunk);
    assert!((actual_localtime - now_local_ts).abs() < 5_000_000);
    let actual_localts = date_eval_ts("localtimestamp", &arena, expr_ts, &[], &chunk);
    assert!((actual_localts - now_local_ts).abs() < 5_000_000);

    let now_utc = Utc::now().naive_utc();
    let now_utc_ts = now_utc.and_utc().timestamp_micros();
    let actual_utc = date_eval_ts("utc_timestamp", &arena, expr_ts, &[], &chunk);
    assert!((actual_utc - now_utc_ts).abs() < 5_000_000);

    let utc_time = now_utc.time();
    let expected_utc_time = base.and_time(utc_time).and_utc().timestamp_micros();
    let actual_utc_time = date_eval_ts("utc_time", &arena, expr_ts, &[], &chunk);
    assert!((actual_utc_time - expected_utc_time).abs() < 5_000_000);
}

// ---------------------------------------------------------------------------
// Tests migrated from dev/test/sql/test_time_fn
// ---------------------------------------------------------------------------

#[test]
fn test_week_iso_year_boundary() {
    // 2023-01-01 (Sunday) is ISO week 52 of 2022
    // 2023-01-02 (Monday) is ISO week 1 of 2023
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_i64 = common::typed_null(&mut arena, DataType::Int64);

    let d1 = common::literal_string(&mut arena, "2023-01-01");
    assert_eq!(date_eval_i64("week_iso", &arena, expr_i64, &[d1], &chunk), 52);

    let d2 = common::literal_string(&mut arena, "2023-01-02");
    assert_eq!(date_eval_i64("week_iso", &arena, expr_i64, &[d2], &chunk), 1);

    let d3 = common::literal_string(&mut arena, "2023-01-03");
    assert_eq!(date_eval_i64("week_iso", &arena, expr_i64, &[d3], &chunk), 1);

    let empty = common::literal_string(&mut arena, "");
    let arr = eval_date_function("week_iso", &arena, expr_i64, &[empty], &chunk).unwrap();
    let arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(arr.is_null(0));

    let null_val = common::typed_null(&mut arena, DataType::Utf8);
    let arr = eval_date_function("week_iso", &arena, expr_i64, &[null_val], &chunk).unwrap();
    let arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(arr.is_null(0));
}

#[test]
fn test_dayofweek_iso_values() {
    // ISO: Mon=1 … Sun=7
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_i64 = common::typed_null(&mut arena, DataType::Int64);

    let d1 = common::literal_string(&mut arena, "2023-01-01"); // Sunday
    assert_eq!(date_eval_i64("dayofweek_iso", &arena, expr_i64, &[d1], &chunk), 7);

    let d2 = common::literal_string(&mut arena, "2023-01-02"); // Monday
    assert_eq!(date_eval_i64("dayofweek_iso", &arena, expr_i64, &[d2], &chunk), 1);

    let d3 = common::literal_string(&mut arena, "2023-01-03"); // Tuesday
    assert_eq!(date_eval_i64("dayofweek_iso", &arena, expr_i64, &[d3], &chunk), 2);

    let empty = common::literal_string(&mut arena, "");
    let arr = eval_date_function("dayofweek_iso", &arena, expr_i64, &[empty], &chunk).unwrap();
    let arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(arr.is_null(0));

    let null_val = common::typed_null(&mut arena, DataType::Utf8);
    let arr = eval_date_function("dayofweek_iso", &arena, expr_i64, &[null_val], &chunk).unwrap();
    let arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(arr.is_null(0));
}

#[test]
fn test_weekday_values() {
    // MySQL weekday: Mon=0 … Sun=6
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_i64 = common::typed_null(&mut arena, DataType::Int64);

    let d1 = common::literal_string(&mut arena, "2023-01-01"); // Sunday
    assert_eq!(date_eval_i64("weekday", &arena, expr_i64, &[d1], &chunk), 6);

    let d2 = common::literal_string(&mut arena, "2023-01-02"); // Monday
    assert_eq!(date_eval_i64("weekday", &arena, expr_i64, &[d2], &chunk), 0);

    let d3 = common::literal_string(&mut arena, "2023-01-03"); // Tuesday
    assert_eq!(date_eval_i64("weekday", &arena, expr_i64, &[d3], &chunk), 1);

    let empty = common::literal_string(&mut arena, "");
    let arr = eval_date_function("weekday", &arena, expr_i64, &[empty], &chunk).unwrap();
    let arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(arr.is_null(0));
}

#[test]
fn test_timestampadd_millisecond() {
    // timestampadd(MILLISECOND, 1, '2019-01-02 00:00:00') => 2019-01-02 00:00:00.001000
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_ts = common::typed_null(&mut arena, DataType::Timestamp(TimeUnit::Microsecond, None));

    let unit = common::literal_string(&mut arena, "millisecond");
    let interval = common::literal_i64(&mut arena, 1);
    let dt = common::literal_string(&mut arena, "2019-01-02 00:00:00");
    assert_eq!(
        date_eval_ts("timestampadd", &arena, expr_ts, &[unit, interval, dt], &chunk),
        dt_micros("2019-01-02 00:00:00") + 1_000
    );
}

#[test]
fn test_timestampdiff_millisecond() {
    // timestampdiff(MILLISECOND, '2003-02-01 00:00:00', '2003-05-01 12:05:55') => 7733155000
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_i64 = common::typed_null(&mut arena, DataType::Int64);

    let unit = common::literal_string(&mut arena, "millisecond");
    let dt1 = common::literal_string(&mut arena, "2003-02-01 00:00:00");
    let dt2 = common::literal_string(&mut arena, "2003-05-01 12:05:55");
    assert_eq!(
        date_eval_i64("timestampdiff", &arena, expr_i64, &[unit, dt1, dt2], &chunk),
        7_733_155_000
    );
}

// ---------------------------------------------------------------------------
// Helpers for sub-second precision
// ---------------------------------------------------------------------------

fn dt_micros_us(s: &str) -> i64 {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
        .unwrap_or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").unwrap())
        .and_utc()
        .timestamp_micros()
}

// ---------------------------------------------------------------------------
// test_date_trunc — migrated from dev/test/sql/test_function/R/test_date_trunc
// ---------------------------------------------------------------------------

#[test]
fn test_date_trunc_specific_values() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_ts = common::typed_null(&mut arena, DataType::Timestamp(TimeUnit::Microsecond, None));

    macro_rules! trunc {
        ($unit:expr, $dt:expr, $expected:expr) => {{
            let unit = common::literal_string(&mut arena, $unit);
            let dt = common::literal_string(&mut arena, $dt);
            assert_eq!(
                date_eval_ts("date_trunc", &arena, expr_ts, &[unit, dt], &chunk),
                dt_micros_us($expected),
                "date_trunc('{}', '{}')",
                $unit,
                $dt
            );
        }};
    }

    trunc!("year",        "2023-10-31 23:59:59.001002", "2023-01-01 00:00:00");
    trunc!("year",        "2023-10-31",                 "2023-01-01 00:00:00");
    trunc!("quarter",     "2023-10-31 23:59:59.001002", "2023-10-01 00:00:00");
    trunc!("quarter",     "2023-10-31",                 "2023-10-01 00:00:00");
    trunc!("quarter",     "2023-09-15 23:59:59.001002", "2023-07-01 00:00:00");
    trunc!("quarter",     "2023-09-15",                 "2023-07-01 00:00:00");
    trunc!("month",       "2023-10-31 23:59:59.001002", "2023-10-01 00:00:00");
    trunc!("month",       "2023-10-31",                 "2023-10-01 00:00:00");
    trunc!("week",        "2023-10-31 23:59:59.001002", "2023-10-30 00:00:00");
    trunc!("week",        "2023-10-31",                 "2023-10-30 00:00:00");
    trunc!("day",         "2023-10-31 23:59:59.001002", "2023-10-31 00:00:00");
    trunc!("day",         "2023-10-31",                 "2023-10-31 00:00:00");
    trunc!("hour",        "2023-10-31 23:59:59.001002", "2023-10-31 23:00:00");
    trunc!("hour",        "2023-10-31",                 "2023-10-31 00:00:00");
    trunc!("minute",      "2023-10-31 23:59:59.001002", "2023-10-31 23:59:00");
    trunc!("minute",      "2023-10-31",                 "2023-10-31 00:00:00");
    trunc!("second",      "2023-10-31 23:59:59.001002", "2023-10-31 23:59:59");
    trunc!("second",      "2023-10-31",                 "2023-10-31 00:00:00");
    trunc!("millisecond", "2023-10-31 23:59:59.001002", "2023-10-31 23:59:59.001000");
    trunc!("millisecond", "2023-10-31",                 "2023-10-31 00:00:00");
    trunc!("microsecond", "2023-10-31 23:59:59.001002", "2023-10-31 23:59:59.001002");
    trunc!("microsecond", "2023-10-31",                 "2023-10-31 00:00:00");
}

// ---------------------------------------------------------------------------
// test_days_add — migrated from dev/test/sql/test_function/R/test_days_add
// (INTERVAL x UNIT is lowered by FE to the corresponding _add function)
// ---------------------------------------------------------------------------

#[test]
fn test_days_add_all_intervals() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_ts = common::typed_null(&mut arena, DataType::Timestamp(TimeUnit::Microsecond, None));

    macro_rules! add {
        ($fn:expr, $dt:expr, $n:expr, $expected:expr) => {{
            let dt = common::literal_string(&mut arena, $dt);
            let n = common::literal_i64(&mut arena, $n);
            assert_eq!(
                date_eval_ts($fn, &arena, expr_ts, &[dt, n], &chunk),
                dt_micros_us($expected),
                "{}('{}', {})",
                $fn,
                $dt,
                $n
            );
        }};
    }

    // days_add / adddate with integer (adds days)
    add!("days_add", "2023-10-31 23:59:59", 1,    "2023-11-01 23:59:59");
    add!("days_add", "2023-10-31 23:59:59", 1000,  "2026-07-27 23:59:59");

    // INTERVAL n YEAR → years_add
    add!("years_add", "2023-10-31 23:59:59", 1,   "2024-10-31 23:59:59");
    add!("years_add", "2023-10-31 23:59:59", 100,  "2123-10-31 23:59:59");

    // INTERVAL n MONTH → months_add (month-end clamping)
    add!("months_add", "2023-10-31 23:59:59", 1,  "2023-11-30 23:59:59");
    add!("months_add", "2023-10-31 23:59:59", 11, "2024-09-30 23:59:59");
    add!("months_add", "2023-10-31 23:59:59", 25, "2025-11-30 23:59:59");

    // INTERVAL n DAY → days_add
    add!("days_add", "2023-10-31 23:59:59", 1,   "2023-11-01 23:59:59");
    add!("days_add", "2023-10-31 23:59:59", 15,  "2023-11-15 23:59:59");
    add!("days_add", "2023-10-31 23:59:59", 100, "2024-02-08 23:59:59");
    add!("days_add", "2023-10-31 23:59:59", 1000,"2026-07-27 23:59:59");

    // INTERVAL n HOUR → hours_add
    add!("hours_add", "2023-10-31 23:59:59", 1,  "2023-11-01 00:59:59");
    add!("hours_add", "2023-10-31 23:59:59", 12, "2023-11-01 11:59:59");
    add!("hours_add", "2023-10-31 23:59:59", 25, "2023-11-02 00:59:59");

    // INTERVAL n MINUTE → minutes_add
    add!("minutes_add", "2023-10-31 23:59:59", 1,  "2023-11-01 00:00:59");
    add!("minutes_add", "2023-10-31 23:59:59", 30, "2023-11-01 00:29:59");
    add!("minutes_add", "2023-10-31 23:59:59", 80, "2023-11-01 01:19:59");

    // INTERVAL n SECOND → seconds_add
    add!("seconds_add", "2023-10-31 23:59:59", 1,  "2023-11-01 00:00:00");
    add!("seconds_add", "2023-10-31 23:59:59", 30, "2023-11-01 00:00:29");
    add!("seconds_add", "2023-10-31 23:59:59", 70, "2023-11-01 00:01:09");

    // INTERVAL n MILLISECOND → milliseconds_add
    add!("milliseconds_add", "2023-10-31 23:59:59", 1,    "2023-10-31 23:59:59.001000");
    add!("milliseconds_add", "2023-10-31 23:59:59", 500,  "2023-10-31 23:59:59.500000");
    add!("milliseconds_add", "2023-10-31 23:59:59", 3000, "2023-11-01 00:00:02");

    // INTERVAL n MICROSECOND → microseconds_add
    add!("microseconds_add", "2023-10-31 23:59:59", 1,    "2023-10-31 23:59:59.000001");
    add!("microseconds_add", "2023-10-31 23:59:59", 500,  "2023-10-31 23:59:59.000500");
    add!("microseconds_add", "2023-10-31 23:59:59", 3000, "2023-10-31 23:59:59.003000");
}

// ---------------------------------------------------------------------------
// test_date_format — %f microsecond specifier
// (migrated from dev/test/sql/test_function/R/test_date_format)
// ---------------------------------------------------------------------------

#[test]
fn test_date_format_microseconds() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_str = common::typed_null(&mut arena, DataType::Utf8);

    let dt = common::literal_string(&mut arena, "2023-10-11 00:00:01.030");
    let fmt = common::literal_string(&mut arena, "%Y-%m-%d %H:%i:%s.%f");
    assert_eq!(
        date_eval_str("date_format", &arena, expr_str, &[dt, fmt], &chunk),
        "2023-10-11 00:00:01.030000"
    );
}

