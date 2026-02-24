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
use crate::common::ids::SlotId;
use crate::exec::chunk::{Chunk, field_with_slot_id};
use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};
use arrow::array::{ArrayRef, Date32Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{Local, NaiveDate, NaiveDateTime, TimeZone, Timelike};
use std::sync::Arc;

use super::eval_date_function;

pub fn chunk_len_1() -> Chunk {
    let array = Arc::new(Int64Array::from(vec![1])) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![field_with_slot_id(
        Field::new("dummy", DataType::Int64, false),
        SlotId::new(1),
    )]));
    let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
    Chunk::new(batch)
}

pub fn literal_i64(arena: &mut ExprArena, v: i64) -> ExprId {
    arena.push(ExprNode::Literal(LiteralValue::Int64(v)))
}

pub fn literal_f64(arena: &mut ExprArena, v: f64) -> ExprId {
    arena.push(ExprNode::Literal(LiteralValue::Float64(v)))
}

pub fn literal_string(arena: &mut ExprArena, v: &str) -> ExprId {
    arena.push(ExprNode::Literal(LiteralValue::Utf8(v.to_string())))
}

pub fn typed_null(arena: &mut ExprArena, data_type: DataType) -> ExprId {
    arena.push_typed(ExprNode::Literal(LiteralValue::Null), data_type)
}

pub fn dt_micros(s: &str) -> i64 {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
        .unwrap()
        .and_utc()
        .timestamp_micros()
}

pub fn date32_from_ymd(year: i32, month: u32, day: u32) -> i32 {
    let date = NaiveDate::from_ymd_opt(year, month, day).unwrap();
    let unix_epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    (date - unix_epoch).num_days() as i32
}

fn date_eval_ts(
    name: &str,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
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
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> i64 {
    let arr = eval_date_function(name, arena, expr, args, chunk).unwrap();
    let arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
    arr.value(0)
}

fn date_eval_date32(
    name: &str,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> i32 {
    let arr = eval_date_function(name, arena, expr, args, chunk).unwrap();
    let arr = arr.as_any().downcast_ref::<Date32Array>().unwrap();
    arr.value(0)
}

fn date_eval_str(
    name: &str,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> String {
    let arr = eval_date_function(name, arena, expr, args, chunk).unwrap();
    let arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
    arr.value(0).to_string()
}

pub fn assert_date_function_logic(name: &str) {
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
    let chunk = chunk_len_1();
    let expr_ts = typed_null(&mut arena, DataType::Timestamp(TimeUnit::Microsecond, None));
    let expr_i64 = typed_null(&mut arena, DataType::Int64);
    let expr_date = typed_null(&mut arena, DataType::Date32);
    let expr_str = typed_null(&mut arena, DataType::Utf8);

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
            let dt = literal_string(&mut arena, "2020-01-02 03:04:05");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[dt], &chunk), 2);
        }
        "dayname" => {
            let dt = literal_string(&mut arena, "2020-01-02 03:04:05");
            assert_eq!(
                date_eval_str(name, &arena, expr_str, &[dt], &chunk),
                "Thursday"
            );
        }
        "dayofweek" => {
            let dt = literal_string(&mut arena, "2020-01-02 03:04:05");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[dt], &chunk), 5);
        }
        "dayofweek_iso" => {
            let dt = literal_string(&mut arena, "2020-01-02 03:04:05");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[dt], &chunk), 4);
        }
        "dayofyear" => {
            let dt = literal_string(&mut arena, "2020-01-02 03:04:05");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[dt], &chunk), 2);
        }
        "weekday" => {
            let dt = literal_string(&mut arena, "2020-01-02 03:04:05");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[dt], &chunk), 3);
        }
        "month" => {
            let dt = literal_string(&mut arena, "2020-01-02 03:04:05");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[dt], &chunk), 1);
        }
        "monthname" => {
            let dt = literal_string(&mut arena, "2020-01-02 03:04:05");
            assert_eq!(
                date_eval_str(name, &arena, expr_str, &[dt], &chunk),
                "January"
            );
        }
        "quarter" => {
            let dt = literal_string(&mut arena, "2020-01-02 03:04:05");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[dt], &chunk), 1);
        }
        "week" => {
            let dt = literal_string(&mut arena, "2020-01-02 03:04:05");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[dt], &chunk), 1);
        }
        "year" => {
            let dt = literal_string(&mut arena, "2020-01-02 03:04:05");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[dt], &chunk), 2020);
        }
        "hour" => {
            let dt = literal_string(&mut arena, "2020-01-02 03:04:05");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[dt], &chunk), 3);
        }
        "minute" => {
            let dt = literal_string(&mut arena, "2020-01-02 03:04:05");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[dt], &chunk), 4);
        }
        "second" => {
            let dt = literal_string(&mut arena, "2020-01-02 03:04:05");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[dt], &chunk), 5);
        }
        "datediff" | "days_diff" => {
            let d1 = literal_string(&mut arena, "2020-01-03 00:00:00");
            let d2 = literal_string(&mut arena, "2020-01-01 00:00:00");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[d1, d2], &chunk), 2);
        }
        "weeks_diff" => {
            let d1 = literal_string(&mut arena, "2020-01-15 00:00:00");
            let d2 = literal_string(&mut arena, "2020-01-01 00:00:00");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[d1, d2], &chunk), 2);
        }
        "months_diff" => {
            let d1 = literal_string(&mut arena, "2020-03-15 00:00:00");
            let d2 = literal_string(&mut arena, "2020-01-15 00:00:00");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[d1, d2], &chunk), 2);
        }
        "years_diff" => {
            let d1 = literal_string(&mut arena, "2021-01-15 00:00:00");
            let d2 = literal_string(&mut arena, "2020-01-15 00:00:00");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[d1, d2], &chunk), 1);
            let leap_l = literal_string(&mut arena, "2026-02-28");
            let leap_r = literal_string(&mut arena, "2024-02-29");
            assert_eq!(
                date_eval_i64(name, &arena, expr_i64, &[leap_l, leap_r], &chunk),
                1
            );
        }
        "hours_diff" => {
            let d1 = literal_string(&mut arena, "2020-01-01 03:00:00");
            let d2 = literal_string(&mut arena, "2020-01-01 01:00:00");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[d1, d2], &chunk), 2);
        }
        "minutes_diff" => {
            let d1 = literal_string(&mut arena, "2020-01-01 00:02:00");
            let d2 = literal_string(&mut arena, "2020-01-01 00:01:00");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[d1, d2], &chunk), 1);
        }
        "seconds_diff" => {
            let d1 = literal_string(&mut arena, "2020-01-01 00:00:03");
            let d2 = literal_string(&mut arena, "2020-01-01 00:00:01");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[d1, d2], &chunk), 2);
        }
        "quarters_diff" => {
            let d1 = literal_string(&mut arena, "2020-05-01 00:00:00");
            let d2 = literal_string(&mut arena, "2020-02-01 00:00:00");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[d1, d2], &chunk), 1);
        }
        "date" | "to_date" | "to_tera_date" => {
            let dt = literal_string(&mut arena, "2020-01-02 03:04:05");
            assert_eq!(
                date_eval_date32(name, &arena, expr_date, &[dt], &chunk),
                date32_from_ymd(2020, 1, 2)
            );
        }
        "timestamp" | "to_datetime" => {
            let dt = literal_string(&mut arena, "2020-01-02 03:04:05");
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[dt], &chunk),
                dt_micros("2020-01-02 03:04:05")
            );
        }
        "date_add" | "days_add" => {
            let base = literal_string(&mut arena, "2020-01-01 00:00:00");
            let n = literal_i64(&mut arena, 2);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-01-03 00:00:00")
            );
        }
        "date_sub" | "days_sub" => {
            let base = literal_string(&mut arena, "2020-01-03 00:00:00");
            let n = literal_i64(&mut arena, 2);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-01-01 00:00:00")
            );
        }
        "weeks_add" => {
            let base = literal_string(&mut arena, "2020-01-01 00:00:00");
            let n = literal_i64(&mut arena, 1);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-01-08 00:00:00")
            );
        }
        "weeks_sub" => {
            let base = literal_string(&mut arena, "2020-01-08 00:00:00");
            let n = literal_i64(&mut arena, 1);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-01-01 00:00:00")
            );
        }
        "hours_add" => {
            let base = literal_string(&mut arena, "2020-01-01 01:00:00");
            let n = literal_i64(&mut arena, 2);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-01-01 03:00:00")
            );
        }
        "hours_sub" => {
            let base = literal_string(&mut arena, "2020-01-01 03:00:00");
            let n = literal_i64(&mut arena, 2);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-01-01 01:00:00")
            );
        }
        "minutes_add" => {
            let base = literal_string(&mut arena, "2020-01-01 00:01:00");
            let n = literal_i64(&mut arena, 1);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-01-01 00:02:00")
            );
        }
        "minutes_sub" => {
            let base = literal_string(&mut arena, "2020-01-01 00:02:00");
            let n = literal_i64(&mut arena, 1);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-01-01 00:01:00")
            );
        }
        "seconds_add" => {
            let base = literal_string(&mut arena, "2020-01-01 00:00:01");
            let n = literal_i64(&mut arena, 2);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-01-01 00:00:03")
            );
        }
        "seconds_sub" => {
            let base = literal_string(&mut arena, "2020-01-01 00:00:03");
            let n = literal_i64(&mut arena, 2);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-01-01 00:00:01")
            );
        }
        "milliseconds_add" => {
            let base = literal_string(&mut arena, "2020-01-01 00:00:00");
            let n = literal_i64(&mut arena, 1500);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-01-01 00:00:01") + 500_000
            );
        }
        "milliseconds_diff" => {
            let d1 = literal_string(&mut arena, "2020-01-01 00:00:03");
            let d2 = literal_string(&mut arena, "2020-01-01 00:00:01");
            assert_eq!(
                date_eval_i64(name, &arena, expr_i64, &[d1, d2], &chunk),
                2000
            );
        }
        "milliseconds_sub" => {
            let base = literal_string(&mut arena, "2020-01-01 00:00:02");
            let n = literal_i64(&mut arena, 1500);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-01-01 00:00:00") + 500_000
            );
        }
        "microseconds_add" => {
            let base = literal_string(&mut arena, "2020-01-01 00:00:00");
            let n = literal_i64(&mut arena, 1000);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-01-01 00:00:00") + 1000
            );
        }
        "microseconds_sub" => {
            let base = literal_string(&mut arena, "2020-01-01 00:00:01");
            let n = literal_i64(&mut arena, 1000);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-01-01 00:00:01") - 1000
            );
        }
        "add_months" | "months_add" => {
            let base = literal_string(&mut arena, "2020-01-15 00:00:00");
            let n = literal_i64(&mut arena, 2);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-03-15 00:00:00")
            );
        }
        "months_sub" => {
            let base = literal_string(&mut arena, "2020-03-15 00:00:00");
            let n = literal_i64(&mut arena, 1);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-02-15 00:00:00")
            );
        }
        "years_add" => {
            let base = literal_string(&mut arena, "2020-01-15 00:00:00");
            let n = literal_i64(&mut arena, 1);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2021-01-15 00:00:00")
            );
        }
        "years_sub" => {
            let base = literal_string(&mut arena, "2021-01-15 00:00:00");
            let n = literal_i64(&mut arena, 1);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-01-15 00:00:00")
            );
        }
        "quarters_add" => {
            let base = literal_string(&mut arena, "2020-01-15 00:00:00");
            let n = literal_i64(&mut arena, 1);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-04-15 00:00:00")
            );
        }
        "quarters_sub" => {
            let base = literal_string(&mut arena, "2020-04-15 00:00:00");
            let n = literal_i64(&mut arena, 1);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[base, n], &chunk),
                dt_micros("2020-01-15 00:00:00")
            );
        }
        "date_format" => {
            let dt = literal_string(&mut arena, "2020-01-02 03:04:05");
            let fmt = literal_string(&mut arena, "%Y-%m-%d");
            assert_eq!(
                date_eval_str(name, &arena, expr_str, &[dt, fmt], &chunk),
                "2020-01-02"
            );
        }
        "time_format" => {
            let dt = literal_string(&mut arena, "12:34:56");
            let fmt = literal_string(&mut arena, "%H:%i:%s");
            assert_eq!(
                date_eval_str(name, &arena, expr_str, &[dt, fmt], &chunk),
                "00:00:00"
            );
        }
        "str_to_date" => {
            let s = literal_string(&mut arena, "2020-01-02");
            let fmt = literal_string(&mut arena, "%Y-%m-%d");
            assert_eq!(
                date_eval_date32(name, &arena, expr_date, &[s, fmt], &chunk),
                date32_from_ymd(2020, 1, 2)
            );
        }
        "date_trunc" | "date_floor" => {
            let unit = literal_string(&mut arena, "month");
            let dt = literal_string(&mut arena, "2020-01-15 12:34:56");
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[unit, dt], &chunk),
                dt_micros("2020-01-01 00:00:00")
            );
        }
        "alignment_timestamp" => {
            let unit = literal_string(&mut arena, "day");
            let dt = literal_string(&mut arena, "2020-01-15 12:34:56");
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[unit, dt], &chunk),
                dt_micros("2020-01-15 00:00:00")
            );
        }
        "time_slice" | "date_slice" => {
            let dt = literal_string(&mut arena, "2020-01-02 03:04:05");
            let interval = literal_i64(&mut arena, 1);
            let unit = literal_string(&mut arena, "hour");
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[dt, interval, unit], &chunk),
                dt_micros("2020-01-02 03:00:00")
            );
        }
        "timestampadd" => {
            let unit = literal_string(&mut arena, "day");
            let interval = literal_i64(&mut arena, 2);
            let dt = literal_string(&mut arena, "2020-01-01 00:00:00");
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[unit, interval, dt], &chunk),
                dt_micros("2020-01-03 00:00:00")
            );
        }
        "timestampdiff" => {
            let unit = literal_string(&mut arena, "hour");
            let dt1 = literal_string(&mut arena, "2020-01-01 00:00:00");
            let dt2 = literal_string(&mut arena, "2020-01-01 03:00:00");
            assert_eq!(
                date_eval_i64(name, &arena, expr_i64, &[unit, dt1, dt2], &chunk),
                3
            );
        }
        "timediff" => {
            let t1 = literal_string(&mut arena, "2020-01-02 03:00:00");
            let t2 = literal_string(&mut arena, "2020-01-02 01:00:00");
            assert_eq!(
                date_eval_str(name, &arena, expr_str, &[t1, t2], &chunk),
                "02:00:00"
            );
        }
        "time_to_sec" => {
            let t = literal_string(&mut arena, "01:01:01");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[t], &chunk), 3661);
        }
        "sec_to_time" => {
            let s = literal_i64(&mut arena, 3661);
            assert_eq!(
                date_eval_str(name, &arena, expr_str, &[s], &chunk),
                "01:01:01"
            );
        }
        "to_days" => {
            let d = literal_string(&mut arena, "1970-01-02");
            let out = date_eval_i64(name, &arena, expr_i64, &[d], &chunk);
            assert!(out > 0);
        }
        "to_iso8601" => {
            let d = literal_string(&mut arena, "2020-01-02 03:04:05.1234");
            assert_eq!(
                date_eval_str(name, &arena, expr_str, &[d], &chunk),
                "2020-01-02T03:04:05.123400"
            );
        }
        "from_days" => {
            let days = literal_i64(&mut arena, 719529);
            assert_eq!(
                date_eval_date32(name, &arena, expr_date, &[days], &chunk),
                date32_from_ymd(1970, 1, 2)
            );
        }
        "last_day" => {
            let d = literal_string(&mut arena, "2020-02-10");
            assert_eq!(
                date_eval_date32(name, &arena, expr_date, &[d], &chunk),
                date32_from_ymd(2020, 2, 29)
            );
        }
        "makedate" => {
            let y = literal_i64(&mut arena, 2020);
            let day = literal_i64(&mut arena, 32);
            assert_eq!(
                date_eval_date32(name, &arena, expr_date, &[y, day], &chunk),
                date32_from_ymd(2020, 2, 1)
            );
        }
        "next_day" => {
            let base = literal_string(&mut arena, "2020-01-02");
            let wd = literal_string(&mut arena, "Mon");
            assert_eq!(
                date_eval_date32(name, &arena, expr_date, &[base, wd], &chunk),
                date32_from_ymd(2020, 1, 6)
            );
        }
        "previous_day" => {
            let base = literal_string(&mut arena, "2020-01-02");
            let wd = literal_string(&mut arena, "Mon");
            assert_eq!(
                date_eval_date32(name, &arena, expr_date, &[base, wd], &chunk),
                date32_from_ymd(2019, 12, 30)
            );
        }
        "convert_tz" => {
            let dt = literal_string(&mut arena, "2020-01-01 00:00:00");
            let from = literal_string(&mut arena, "+00:00");
            let to = literal_string(&mut arena, "+08:00");
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[dt, from, to], &chunk),
                dt_micros("2020-01-01 08:00:00")
            );
        }
        "from_unixtime" => {
            let v = literal_i64(&mut arena, 1_000_000);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[v], &chunk),
                dt_micros("1970-01-01 00:00:01")
            );
        }
        "from_unixtime_ms" => {
            let v = literal_i64(&mut arena, 1000);
            assert_eq!(
                date_eval_ts(name, &arena, expr_ts, &[v], &chunk),
                dt_micros("1970-01-01 00:00:01")
            );
        }
        "hour_from_unixtime" => {
            let v = literal_i64(&mut arena, 5 * 3600);
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
            let dt = literal_string(&mut arena, "1970-01-01 00:00:01");
            assert_eq!(date_eval_i64(name, &arena, expr_i64, &[dt], &chunk), 1);
        }
        "substitute" => {
            let s = literal_string(&mut arena, "2020-01-02");
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
