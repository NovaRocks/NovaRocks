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
#[cfg(test)]
use super::common::{BC_EPOCH_JULIAN, julian_from_date, naive_to_date32};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::ArrayRef;
use std::collections::HashMap;

#[derive(Clone, Copy)]
pub struct FunctionMeta {
    pub name: &'static str,
    pub min_args: usize,
    pub max_args: usize,
}

pub fn register(map: &mut HashMap<&'static str, crate::exec::expr::function::FunctionKind>) {
    for (name, canonical) in DATE_FUNCTIONS {
        map.insert(
            *name,
            crate::exec::expr::function::FunctionKind::Date(*canonical),
        );
    }
}

pub fn metadata(name: &str) -> Option<FunctionMeta> {
    DATE_METADATA.iter().find(|m| m.name == name).copied()
}

pub fn eval_date_function(
    name: &str,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let canonical = DATE_FUNCTIONS
        .iter()
        .find_map(|(alias, target)| (*alias == name).then_some(*target))
        .unwrap_or(name);

    match canonical {
        "add_months" => super::add_months::eval_add_months(arena, expr, args, chunk),
        "date_add" => super::date_add::eval_date_add(arena, expr, args, chunk),
        "date_diff" => super::datediff::eval_date_diff(arena, expr, args, chunk),
        "alignment_timestamp" => {
            super::date_trunc::eval_alignment_timestamp(arena, expr, args, chunk)
        }
        "convert_tz" => super::convert_tz::eval_convert_tz(arena, expr, args, chunk),
        "current_date" => super::current_date::eval_current_date(arena, expr, args, chunk),
        "current_time" => super::current_time::eval_current_time(arena, expr, args, chunk),
        "current_timestamp" => {
            super::current_timestamp::eval_current_timestamp(arena, expr, args, chunk)
        }
        "date" => super::date::eval_date(arena, expr, args, chunk),
        "date_floor" => super::date_trunc::eval_date_floor(arena, expr, args, chunk),
        "date_format" => super::date_format::eval_date_format(arena, expr, args, chunk),
        "date_slice" => super::time_slice::eval_date_slice(arena, expr, args, chunk),
        "date_sub" => super::date_add::eval_date_sub(arena, expr, args, chunk),
        "date_trunc" => super::date_trunc::eval_date_trunc(arena, expr, args, chunk),
        "datediff" => super::datediff::eval_datediff(arena, expr, args, chunk),
        "day" => super::date_parts::eval_day(arena, expr, args, chunk),
        "dayname" => super::date_parts::eval_dayname(arena, expr, args, chunk),
        "dayofweek" => super::date_parts::eval_dayofweek(arena, expr, args, chunk),
        "dayofweek_iso" => super::date_parts::eval_dayofweek_iso(arena, expr, args, chunk),
        "dayofyear" => super::date_parts::eval_dayofyear(arena, expr, args, chunk),
        "days_add" => super::date_add::eval_days_add(arena, expr, args, chunk),
        "days_diff" => super::datediff::eval_days_diff(arena, expr, args, chunk),
        "days_sub" => super::date_add::eval_days_sub(arena, expr, args, chunk),
        "from_days" => super::from_days::eval_from_days(arena, expr, args, chunk),
        "from_unixtime" => super::from_unixtime::eval_from_unixtime(arena, expr, args, chunk),
        "from_unixtime_ms" => super::from_unixtime::eval_from_unixtime_ms(arena, expr, args, chunk),
        "hour" => super::date_parts::eval_hour(arena, expr, args, chunk),
        "hour_from_unixtime" => {
            super::hour_from_unixtime::eval_hour_from_unixtime(arena, expr, args, chunk)
        }
        "hours_add" => super::seconds_add::eval_hours_add(arena, expr, args, chunk),
        "hours_diff" => super::datediff::eval_hours_diff(arena, expr, args, chunk),
        "hours_sub" => super::seconds_add::eval_hours_sub(arena, expr, args, chunk),
        "jodatime_format" => super::date_format::eval_jodatime_format(arena, expr, args, chunk),
        "last_day" => super::last_day::eval_last_day(arena, expr, args, chunk),
        "makedate" => super::makedate::eval_makedate(arena, expr, args, chunk),
        "microseconds_add" => super::seconds_add::eval_microseconds_add(arena, expr, args, chunk),
        "microseconds_sub" => super::seconds_add::eval_microseconds_sub(arena, expr, args, chunk),
        "milliseconds_add" => super::seconds_add::eval_milliseconds_add(arena, expr, args, chunk),
        "milliseconds_diff" => super::datediff::eval_milliseconds_diff(arena, expr, args, chunk),
        "milliseconds_sub" => super::seconds_add::eval_milliseconds_sub(arena, expr, args, chunk),
        "minute" => super::date_parts::eval_minute(arena, expr, args, chunk),
        "minutes_add" => super::seconds_add::eval_minutes_add(arena, expr, args, chunk),
        "minutes_diff" => super::datediff::eval_minutes_diff(arena, expr, args, chunk),
        "minutes_sub" => super::seconds_add::eval_minutes_sub(arena, expr, args, chunk),
        "month" => super::date_parts::eval_month(arena, expr, args, chunk),
        "monthname" => super::date_parts::eval_monthname(arena, expr, args, chunk),
        "months_add" => super::add_months::eval_months_add(arena, expr, args, chunk),
        "months_diff" => super::datediff::eval_months_diff(arena, expr, args, chunk),
        "months_sub" => super::add_months::eval_months_sub(arena, expr, args, chunk),
        "next_day" => super::next_day::eval_next_day(arena, expr, args, chunk),
        "previous_day" => super::next_day::eval_previous_day(arena, expr, args, chunk),
        "quarter" => super::date_parts::eval_quarter(arena, expr, args, chunk),
        "quarters_add" => super::add_months::eval_quarters_add(arena, expr, args, chunk),
        "quarters_sub" => super::add_months::eval_quarters_sub(arena, expr, args, chunk),
        "sec_to_time" => super::sec_to_time::eval_sec_to_time(arena, expr, args, chunk),
        "second" => super::date_parts::eval_second(arena, expr, args, chunk),
        "seconds_add" => super::seconds_add::eval_seconds_add(arena, expr, args, chunk),
        "seconds_diff" => super::datediff::eval_seconds_diff(arena, expr, args, chunk),
        "seconds_sub" => super::seconds_add::eval_seconds_sub(arena, expr, args, chunk),
        "str_to_jodatime" => super::date::eval_str_to_jodatime(arena, expr, args, chunk),
        "str_to_date" => super::str_to_date::eval_str_to_date(arena, expr, args, chunk),
        "substitute" => super::substitute::eval_substitute(arena, expr, args, chunk),
        "time_format" => super::time_format::eval_time_format(arena, expr, args, chunk),
        "time_slice" => super::time_slice::eval_time_slice(arena, expr, args, chunk),
        "time_to_sec" => super::time_to_sec::eval_time_to_sec(arena, expr, args, chunk),
        "timediff" => super::timediff::eval_timediff(arena, expr, args, chunk),
        "timestamp" => super::date::eval_timestamp(arena, expr, args, chunk),
        "timestampadd" => super::timestampadd::eval_timestampadd(arena, expr, args, chunk),
        "timestampdiff" => super::timestampdiff::eval_timestampdiff(arena, expr, args, chunk),
        "to_date" => super::date::eval_to_date(arena, expr, args, chunk),
        "to_datetime" => super::date::eval_to_datetime(arena, expr, args, chunk),
        "to_datetime_ntz" => super::date::eval_to_datetime_ntz(arena, expr, args, chunk),
        "to_days" => super::to_days::eval_to_days(arena, expr, args, chunk),
        "to_iso8601" => super::to_iso8601::eval_to_iso8601(arena, expr, args, chunk),
        "to_tera_date" => super::date::eval_to_tera_date(arena, expr, args, chunk),
        "to_tera_timestamp" => super::date::eval_to_tera_timestamp(arena, expr, args, chunk),
        "unix_timestamp" => super::unix_timestamp::eval_unix_timestamp(arena, expr, args, chunk),
        "utc_time" => super::current_time::eval_utc_time(arena, expr, args, chunk),
        "utc_timestamp" => super::current_timestamp::eval_utc_timestamp(arena, expr, args, chunk),
        "week" => super::date_parts::eval_week(arena, expr, args, chunk),
        "weekday" => super::date_parts::eval_weekday(arena, expr, args, chunk),
        "weeks_add" => super::date_add::eval_weeks_add(arena, expr, args, chunk),
        "weeks_diff" => super::datediff::eval_weeks_diff(arena, expr, args, chunk),
        "weeks_sub" => super::date_add::eval_weeks_sub(arena, expr, args, chunk),
        "year" => super::date_parts::eval_year(arena, expr, args, chunk),
        "yearweek" => super::date_parts::eval_yearweek(arena, expr, args, chunk),
        "years_add" => super::add_months::eval_years_add(arena, expr, args, chunk),
        "years_diff" => super::datediff::eval_years_diff(arena, expr, args, chunk),
        "years_sub" => super::add_months::eval_years_sub(arena, expr, args, chunk),
        "quarters_diff" => super::datediff::eval_quarters_diff(arena, expr, args, chunk),
        other => Err(format!("unsupported date function: {}", other)),
    }
}

static DATE_FUNCTIONS: &[(&str, &str)] = &[
    ("add_months", "add_months"),
    ("adddate", "date_add"),
    ("alignment_timestamp", "alignment_timestamp"),
    ("convert_tz", "convert_tz"),
    ("curdate", "current_date"),
    ("current_date", "current_date"),
    ("current_time", "current_time"),
    ("current_timestamp", "current_timestamp"),
    ("curtime", "current_time"),
    ("date", "date"),
    ("date_to_iso8601", "to_iso8601"),
    ("date_add", "date_add"),
    ("date_diff", "date_diff"),
    ("date_floor", "date_floor"),
    ("date_format", "date_format"),
    ("date_slice", "date_slice"),
    ("date_sub", "date_sub"),
    ("date_trunc", "date_trunc"),
    ("datediff", "datediff"),
    ("day", "day"),
    ("dayname", "dayname"),
    ("day_of_week_iso", "dayofweek_iso"),
    ("dayofmonth", "day"),
    ("dayofweek", "dayofweek"),
    ("dayofweek_iso", "dayofweek_iso"),
    ("dayofyear", "dayofyear"),
    ("days_add", "days_add"),
    ("days_diff", "days_diff"),
    ("days_sub", "days_sub"),
    ("from_days", "from_days"),
    ("from_unixtime", "from_unixtime"),
    ("from_unixtime_ms", "from_unixtime_ms"),
    ("hour", "hour"),
    ("hour_from_unixtime", "hour_from_unixtime"),
    ("hours_add", "hours_add"),
    ("hours_diff", "hours_diff"),
    ("hours_sub", "hours_sub"),
    ("format_datetime", "jodatime_format"),
    ("jodatime_format", "jodatime_format"),
    ("last_day", "last_day"),
    ("localtime", "current_timestamp"),
    ("localtimestamp", "current_timestamp"),
    ("makedate", "makedate"),
    ("microseconds_add", "microseconds_add"),
    ("microseconds_sub", "microseconds_sub"),
    ("milliseconds_add", "milliseconds_add"),
    ("milliseconds_diff", "milliseconds_diff"),
    ("milliseconds_sub", "milliseconds_sub"),
    ("minute", "minute"),
    ("minutes_add", "minutes_add"),
    ("minutes_diff", "minutes_diff"),
    ("minutes_sub", "minutes_sub"),
    ("month", "month"),
    ("monthname", "monthname"),
    ("months_add", "months_add"),
    ("months_diff", "months_diff"),
    ("months_sub", "months_sub"),
    ("next_day", "next_day"),
    ("now", "current_timestamp"),
    ("previous_day", "previous_day"),
    ("quarter", "quarter"),
    ("quarters_add", "quarters_add"),
    ("quarters_diff", "quarters_diff"),
    ("quarters_sub", "quarters_sub"),
    ("sec_to_time", "sec_to_time"),
    ("second", "second"),
    ("seconds_add", "seconds_add"),
    ("seconds_diff", "seconds_diff"),
    ("seconds_sub", "seconds_sub"),
    ("str2date", "str_to_date"),
    ("str_to_date", "str_to_date"),
    ("strftime", "date_format"),
    ("subdate", "date_sub"),
    ("substitute", "substitute"),
    ("time_format", "time_format"),
    ("time_slice", "time_slice"),
    ("time_to_sec", "time_to_sec"),
    ("timediff", "timediff"),
    ("timestamp", "timestamp"),
    ("timestampadd", "timestampadd"),
    ("timestampdiff", "timestampdiff"),
    ("parse_datetime", "str_to_jodatime"),
    ("str_to_jodatime", "str_to_jodatime"),
    ("to_date", "to_date"),
    ("to_datetime", "to_datetime"),
    ("to_datetime_ntz", "to_datetime_ntz"),
    ("datetime_to_iso8601", "to_iso8601"),
    ("to_days", "to_days"),
    ("to_iso8601", "to_iso8601"),
    ("to_tera_date", "to_tera_date"),
    ("to_tera_timestamp", "to_tera_timestamp"),
    ("unix_timestamp", "unix_timestamp"),
    ("utc_time", "utc_time"),
    ("utc_timestamp", "utc_timestamp"),
    ("week", "week"),
    ("week_day", "weekday"),
    ("week_iso", "week"),
    ("weekday", "weekday"),
    ("weekofyear", "week"),
    ("weeks_add", "weeks_add"),
    ("weeks_diff", "weeks_diff"),
    ("weeks_sub", "weeks_sub"),
    ("year", "year"),
    ("yearweek", "yearweek"),
    ("years_add", "years_add"),
    ("years_diff", "years_diff"),
    ("years_sub", "years_sub"),
];

static DATE_METADATA: &[FunctionMeta] = &[
    FunctionMeta {
        name: "add_months",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "date_add",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "date_sub",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "date_diff",
        min_args: 3,
        max_args: 3,
    },
    FunctionMeta {
        name: "datediff",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "days_diff",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "day",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "dayname",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "dayofweek",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "dayofweek_iso",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "dayofyear",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "weekday",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "month",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "monthname",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "year",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "yearweek",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "hour",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "minute",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "second",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "week",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "quarter",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "current_date",
        min_args: 0,
        max_args: 0,
    },
    FunctionMeta {
        name: "current_time",
        min_args: 0,
        max_args: 0,
    },
    FunctionMeta {
        name: "current_timestamp",
        min_args: 0,
        max_args: 0,
    },
    FunctionMeta {
        name: "utc_time",
        min_args: 0,
        max_args: 0,
    },
    FunctionMeta {
        name: "utc_timestamp",
        min_args: 0,
        max_args: 0,
    },
    FunctionMeta {
        name: "date",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "to_date",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "to_datetime",
        min_args: 1,
        max_args: 2,
    },
    FunctionMeta {
        name: "to_datetime_ntz",
        min_args: 1,
        max_args: 2,
    },
    FunctionMeta {
        name: "timestamp",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "from_unixtime",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "from_unixtime_ms",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "unix_timestamp",
        min_args: 0,
        max_args: 1,
    },
    FunctionMeta {
        name: "hour_from_unixtime",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "date_format",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "time_format",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "str_to_date",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "date_trunc",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "date_floor",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "date_slice",
        min_args: 3,
        max_args: 4,
    },
    FunctionMeta {
        name: "time_slice",
        min_args: 3,
        max_args: 4,
    },
    FunctionMeta {
        name: "timestampadd",
        min_args: 3,
        max_args: 3,
    },
    FunctionMeta {
        name: "timestampdiff",
        min_args: 3,
        max_args: 3,
    },
    FunctionMeta {
        name: "timediff",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "time_to_sec",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "sec_to_time",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "from_days",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "to_days",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "to_iso8601",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "last_day",
        min_args: 1,
        max_args: 2,
    },
    FunctionMeta {
        name: "makedate",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "next_day",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "previous_day",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "convert_tz",
        min_args: 3,
        max_args: 3,
    },
    FunctionMeta {
        name: "alignment_timestamp",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "to_tera_date",
        min_args: 1,
        max_args: 2,
    },
    FunctionMeta {
        name: "to_tera_timestamp",
        min_args: 1,
        max_args: 2,
    },
    FunctionMeta {
        name: "str_to_jodatime",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "jodatime_format",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "days_add",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "days_sub",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "months_add",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "months_sub",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "months_diff",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "years_add",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "years_sub",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "years_diff",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "weeks_add",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "weeks_sub",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "weeks_diff",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "hours_add",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "hours_sub",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "hours_diff",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "minutes_add",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "minutes_sub",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "minutes_diff",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "seconds_add",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "seconds_sub",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "seconds_diff",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "milliseconds_add",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "milliseconds_diff",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "milliseconds_sub",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "microseconds_add",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "microseconds_sub",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "quarters_add",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "quarters_sub",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "quarters_diff",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "substitute",
        min_args: 1,
        max_args: 1,
    },
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::expr::function::date::test_utils::*;
    use arrow::array::{Array, Date32Array, Int64Array, StringArray, TimestampMicrosecondArray};
    use arrow::datatypes::{DataType, TimeUnit};
    use chrono::{Local, NaiveDate, NaiveDateTime, TimeZone, Timelike, Utc};

    fn eval_ts(name: &str, arena: &ExprArena, expr: ExprId, args: &[ExprId], chunk: &Chunk) -> i64 {
        let arr = eval_date_function(name, arena, expr, args, chunk).unwrap();
        let arr = arr
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        arr.value(0)
    }

    fn eval_i64(
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

    fn eval_date32(
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

    fn eval_str(
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

    #[test]
    fn test_date_parts_and_names() {
        let mut arena = ExprArena::default();
        let dt = literal_string(&mut arena, "2020-01-02 03:04:05");
        let chunk = chunk_len_1();
        let expr_i64 = typed_null(&mut arena, DataType::Int64);
        let expr_str = typed_null(&mut arena, DataType::Utf8);

        assert_eq!(eval_i64("day", &arena, expr_i64, &[dt], &chunk), 2);
        assert_eq!(eval_i64("dayofmonth", &arena, expr_i64, &[dt], &chunk), 2);
        assert_eq!(eval_i64("dayofweek", &arena, expr_i64, &[dt], &chunk), 5);
        assert_eq!(
            eval_i64("dayofweek_iso", &arena, expr_i64, &[dt], &chunk),
            4
        );
        assert_eq!(eval_i64("dayofyear", &arena, expr_i64, &[dt], &chunk), 2);
        assert_eq!(eval_i64("month", &arena, expr_i64, &[dt], &chunk), 1);
        assert_eq!(eval_i64("year", &arena, expr_i64, &[dt], &chunk), 2020);
        assert_eq!(eval_i64("hour", &arena, expr_i64, &[dt], &chunk), 3);
        assert_eq!(eval_i64("minute", &arena, expr_i64, &[dt], &chunk), 4);
        assert_eq!(eval_i64("second", &arena, expr_i64, &[dt], &chunk), 5);
        assert_eq!(eval_i64("quarter", &arena, expr_i64, &[dt], &chunk), 1);
        assert_eq!(eval_i64("week", &arena, expr_i64, &[dt], &chunk), 1);
        assert_eq!(eval_i64("week_iso", &arena, expr_i64, &[dt], &chunk), 1);
        assert_eq!(eval_i64("weekday", &arena, expr_i64, &[dt], &chunk), 3);
        assert_eq!(eval_i64("weekofyear", &arena, expr_i64, &[dt], &chunk), 1);
        assert_eq!(
            eval_str("dayname", &arena, expr_str, &[dt], &chunk),
            "Thursday"
        );
        assert_eq!(
            eval_str("monthname", &arena, expr_str, &[dt], &chunk),
            "January"
        );
    }

    #[test]
    fn test_date_add_sub_variants() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_ts = typed_null(&mut arena, DataType::Timestamp(TimeUnit::Microsecond, None));

        let base_add = literal_string(&mut arena, "2020-01-01 00:00:00");
        let base_sub = literal_string(&mut arena, "2020-01-03 00:00:00");
        let days = literal_i64(&mut arena, 2);

        let expected_add = dt_micros("2020-01-03 00:00:00");
        let expected_sub = dt_micros("2020-01-01 00:00:00");
        assert_eq!(
            eval_ts("date_add", &arena, expr_ts, &[base_add, days], &chunk),
            expected_add
        );
        assert_eq!(
            eval_ts("adddate", &arena, expr_ts, &[base_add, days], &chunk),
            expected_add
        );
        assert_eq!(
            eval_ts("days_add", &arena, expr_ts, &[base_add, days], &chunk),
            expected_add
        );
        assert_eq!(
            eval_ts("date_sub", &arena, expr_ts, &[base_sub, days], &chunk),
            expected_sub
        );
        assert_eq!(
            eval_ts("subdate", &arena, expr_ts, &[base_sub, days], &chunk),
            expected_sub
        );
        assert_eq!(
            eval_ts("days_sub", &arena, expr_ts, &[base_sub, days], &chunk),
            expected_sub
        );

        let base_week = literal_string(&mut arena, "2020-01-01 00:00:00");
        let weeks = literal_i64(&mut arena, 1);
        assert_eq!(
            eval_ts("weeks_add", &arena, expr_ts, &[base_week, weeks], &chunk),
            dt_micros("2020-01-08 00:00:00")
        );
        let base_week_sub = literal_string(&mut arena, "2020-01-08 00:00:00");
        assert_eq!(
            eval_ts(
                "weeks_sub",
                &arena,
                expr_ts,
                &[base_week_sub, weeks],
                &chunk
            ),
            dt_micros("2020-01-01 00:00:00")
        );

        let base_hour = literal_string(&mut arena, "2020-01-01 01:00:00");
        let hours = literal_i64(&mut arena, 2);
        assert_eq!(
            eval_ts("hours_add", &arena, expr_ts, &[base_hour, hours], &chunk),
            dt_micros("2020-01-01 03:00:00")
        );
        let base_hour_sub = literal_string(&mut arena, "2020-01-01 03:00:00");
        assert_eq!(
            eval_ts(
                "hours_sub",
                &arena,
                expr_ts,
                &[base_hour_sub, hours],
                &chunk
            ),
            dt_micros("2020-01-01 01:00:00")
        );

        let base_min = literal_string(&mut arena, "2020-01-01 00:01:00");
        let mins = literal_i64(&mut arena, 1);
        assert_eq!(
            eval_ts("minutes_add", &arena, expr_ts, &[base_min, mins], &chunk),
            dt_micros("2020-01-01 00:02:00")
        );
        let base_min_sub = literal_string(&mut arena, "2020-01-01 00:02:00");
        assert_eq!(
            eval_ts(
                "minutes_sub",
                &arena,
                expr_ts,
                &[base_min_sub, mins],
                &chunk
            ),
            dt_micros("2020-01-01 00:01:00")
        );

        let base_sec = literal_string(&mut arena, "2020-01-01 00:00:01");
        let secs = literal_i64(&mut arena, 2);
        assert_eq!(
            eval_ts("seconds_add", &arena, expr_ts, &[base_sec, secs], &chunk),
            dt_micros("2020-01-01 00:00:03")
        );
        let base_sec_sub = literal_string(&mut arena, "2020-01-01 00:00:03");
        assert_eq!(
            eval_ts(
                "seconds_sub",
                &arena,
                expr_ts,
                &[base_sec_sub, secs],
                &chunk
            ),
            dt_micros("2020-01-01 00:00:01")
        );

        let base_ms = literal_string(&mut arena, "2020-01-01 00:00:00");
        let millis = literal_i64(&mut arena, 1500);
        assert_eq!(
            eval_ts(
                "milliseconds_add",
                &arena,
                expr_ts,
                &[base_ms, millis],
                &chunk
            ),
            dt_micros("2020-01-01 00:00:01") + 500_000
        );
        let base_ms_sub = literal_string(&mut arena, "2020-01-01 00:00:02");
        assert_eq!(
            eval_ts(
                "milliseconds_sub",
                &arena,
                expr_ts,
                &[base_ms_sub, millis],
                &chunk
            ),
            dt_micros("2020-01-01 00:00:00") + 500_000
        );

        let micros = literal_i64(&mut arena, 1000);
        assert_eq!(
            eval_ts(
                "microseconds_add",
                &arena,
                expr_ts,
                &[base_ms, micros],
                &chunk
            ),
            dt_micros("2020-01-01 00:00:00") + 1000
        );
        let base_micro_sub = literal_string(&mut arena, "2020-01-01 00:00:01");
        assert_eq!(
            eval_ts(
                "microseconds_sub",
                &arena,
                expr_ts,
                &[base_micro_sub, micros],
                &chunk
            ),
            dt_micros("2020-01-01 00:00:01") - 1000
        );

        let invalid_date = literal_string(&mut arena, "abcd");
        let one_day = literal_i64(&mut arena, 1);
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

        let valid_date = literal_string(&mut arena, "2024-01-01");
        let invalid_days = literal_string(&mut arena, "x");
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
        let chunk = chunk_len_1();
        let expr_ts = typed_null(&mut arena, DataType::Timestamp(TimeUnit::Microsecond, None));
        let base = literal_string(&mut arena, "2020-01-15 00:00:00");
        let months = literal_i64(&mut arena, 2);
        assert_eq!(
            eval_ts("add_months", &arena, expr_ts, &[base, months], &chunk),
            dt_micros("2020-03-15 00:00:00")
        );
        assert_eq!(
            eval_ts("months_add", &arena, expr_ts, &[base, months], &chunk),
            dt_micros("2020-03-15 00:00:00")
        );

        let base_sub = literal_string(&mut arena, "2020-03-15 00:00:00");
        let one = literal_i64(&mut arena, 1);
        assert_eq!(
            eval_ts("months_sub", &arena, expr_ts, &[base_sub, one], &chunk),
            dt_micros("2020-02-15 00:00:00")
        );

        let years = literal_i64(&mut arena, 1);
        assert_eq!(
            eval_ts("years_add", &arena, expr_ts, &[base, years], &chunk),
            dt_micros("2021-01-15 00:00:00")
        );
        let base_year_sub = literal_string(&mut arena, "2021-01-15 00:00:00");
        assert_eq!(
            eval_ts(
                "years_sub",
                &arena,
                expr_ts,
                &[base_year_sub, years],
                &chunk
            ),
            dt_micros("2020-01-15 00:00:00")
        );

        let quarters = literal_i64(&mut arena, 1);
        assert_eq!(
            eval_ts("quarters_add", &arena, expr_ts, &[base, quarters], &chunk),
            dt_micros("2020-04-15 00:00:00")
        );
        let base_quarter_sub = literal_string(&mut arena, "2020-04-15 00:00:00");
        assert_eq!(
            eval_ts(
                "quarters_sub",
                &arena,
                expr_ts,
                &[base_quarter_sub, quarters],
                &chunk
            ),
            dt_micros("2020-01-15 00:00:00")
        );

        let invalid_base = literal_string(&mut arena, "abcd");
        let one_month = literal_i64(&mut arena, 1);
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

        let valid_base = literal_string(&mut arena, "2024-01-01");
        let invalid_month = literal_string(&mut arena, "x");
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
        let chunk = chunk_len_1();
        let expr_i64 = typed_null(&mut arena, DataType::Int64);

        let d1 = literal_string(&mut arena, "2020-01-03 00:00:00");
        let d2 = literal_string(&mut arena, "2020-01-01 00:00:00");
        assert_eq!(eval_i64("datediff", &arena, expr_i64, &[d1, d2], &chunk), 2);
        assert_eq!(
            eval_i64("days_diff", &arena, expr_i64, &[d1, d2], &chunk),
            2
        );

        let w1 = literal_string(&mut arena, "2020-01-15 00:00:00");
        let w2 = literal_string(&mut arena, "2020-01-01 00:00:00");
        assert_eq!(
            eval_i64("weeks_diff", &arena, expr_i64, &[w1, w2], &chunk),
            2
        );

        let m1 = literal_string(&mut arena, "2020-03-15 00:00:00");
        let m2 = literal_string(&mut arena, "2020-01-15 00:00:00");
        assert_eq!(
            eval_i64("months_diff", &arena, expr_i64, &[m1, m2], &chunk),
            2
        );
        let m3 = literal_string(&mut arena, "2024-01-31 00:00:00");
        let m4 = literal_string(&mut arena, "2024-02-01 00:00:00");
        assert_eq!(
            eval_i64("months_diff", &arena, expr_i64, &[m3, m4], &chunk),
            0
        );
        let m5 = literal_string(&mut arena, "2024-02-01 00:00:00");
        let m6 = literal_string(&mut arena, "2024-01-31 00:00:00");
        assert_eq!(
            eval_i64("months_diff", &arena, expr_i64, &[m5, m6], &chunk),
            0
        );

        let y1 = literal_string(&mut arena, "2021-01-15 00:00:00");
        let y2 = literal_string(&mut arena, "2020-01-15 00:00:00");
        assert_eq!(
            eval_i64("years_diff", &arena, expr_i64, &[y1, y2], &chunk),
            1
        );
        let leap_l = literal_string(&mut arena, "2026-02-28");
        let leap_r = literal_string(&mut arena, "2024-02-29");
        assert_eq!(
            eval_i64("years_diff", &arena, expr_i64, &[leap_l, leap_r], &chunk),
            1
        );

        let h1 = literal_string(&mut arena, "2020-01-01 03:00:00");
        let h2 = literal_string(&mut arena, "2020-01-01 01:00:00");
        assert_eq!(
            eval_i64("hours_diff", &arena, expr_i64, &[h1, h2], &chunk),
            2
        );

        let n1 = literal_string(&mut arena, "2020-01-01 00:02:00");
        let n2 = literal_string(&mut arena, "2020-01-01 00:01:00");
        assert_eq!(
            eval_i64("minutes_diff", &arena, expr_i64, &[n1, n2], &chunk),
            1
        );

        let s1 = literal_string(&mut arena, "2020-01-01 00:00:03");
        let s2 = literal_string(&mut arena, "2020-01-01 00:00:01");
        assert_eq!(
            eval_i64("seconds_diff", &arena, expr_i64, &[s1, s2], &chunk),
            2
        );

        let q1 = literal_string(&mut arena, "2020-05-01 00:00:00");
        let q2 = literal_string(&mut arena, "2020-02-01 00:00:00");
        assert_eq!(
            eval_i64("quarters_diff", &arena, expr_i64, &[q1, q2], &chunk),
            1
        );
    }

    #[test]
    fn test_date_and_datetime_conversions() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_date = typed_null(&mut arena, DataType::Date32);
        let expr_ts = typed_null(&mut arena, DataType::Timestamp(TimeUnit::Microsecond, None));

        let dt = literal_string(&mut arena, "2020-01-02 03:04:05");
        let expected_date = date32_from_ymd(2020, 1, 2);
        assert_eq!(
            eval_date32("date", &arena, expr_date, &[dt], &chunk),
            expected_date
        );
        assert_eq!(
            eval_date32("to_date", &arena, expr_date, &[dt], &chunk),
            expected_date
        );
        assert_eq!(
            eval_date32("to_tera_date", &arena, expr_date, &[dt], &chunk),
            expected_date
        );

        let expected_ts = dt_micros("2020-01-02 03:04:05");
        assert_eq!(
            eval_ts("timestamp", &arena, expr_ts, &[dt], &chunk),
            expected_ts
        );
        assert_eq!(
            eval_ts("to_datetime", &arena, expr_ts, &[dt], &chunk),
            expected_ts
        );
    }

    #[test]
    fn test_format_and_parse() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_str = typed_null(&mut arena, DataType::Utf8);
        let expr_date = typed_null(&mut arena, DataType::Date32);
        let dt = literal_string(&mut arena, "2020-01-02 03:04:05");
        let tm = literal_string(&mut arena, "12:34:56");
        let fmt_date = literal_string(&mut arena, "%Y-%m-%d");
        let fmt_time = literal_string(&mut arena, "%H:%i:%s");

        assert_eq!(
            eval_str("date_format", &arena, expr_str, &[dt, fmt_date], &chunk),
            "2020-01-02"
        );
        assert_eq!(
            eval_str("strftime", &arena, expr_str, &[dt, fmt_date], &chunk),
            "2020-01-02"
        );
        assert_eq!(
            eval_str("time_format", &arena, expr_str, &[tm, fmt_time], &chunk),
            "00:00:00"
        );
        let arr = eval_date_function("time_format", &arena, expr_str, &[dt, fmt_time], &chunk)
            .expect("time_format should evaluate");
        let arr = arr
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray");
        assert!(arr.is_null(0));

        let s = literal_string(&mut arena, "2020-01-02");
        let fmt = literal_string(&mut arena, "%Y-%m-%d");
        let expected_date = date32_from_ymd(2020, 1, 2);
        assert_eq!(
            eval_date32("str_to_date", &arena, expr_date, &[s, fmt], &chunk),
            expected_date
        );
        assert_eq!(
            eval_date32("str2date", &arena, expr_date, &[s, fmt], &chunk),
            expected_date
        );
    }

    #[test]
    fn test_timestampadd_and_timestampdiff() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_ts = typed_null(&mut arena, DataType::Timestamp(TimeUnit::Microsecond, None));
        let expr_i64 = typed_null(&mut arena, DataType::Int64);

        let unit = literal_string(&mut arena, "day");
        let interval = literal_i64(&mut arena, 2);
        let dt = literal_string(&mut arena, "2020-01-01 00:00:00");
        assert_eq!(
            eval_ts(
                "timestampadd",
                &arena,
                expr_ts,
                &[unit, interval, dt],
                &chunk
            ),
            dt_micros("2020-01-03 00:00:00")
        );

        let unit_h = literal_string(&mut arena, "hour");
        let dt1 = literal_string(&mut arena, "2020-01-01 00:00:00");
        let dt2 = literal_string(&mut arena, "2020-01-01 03:00:00");
        assert_eq!(
            eval_i64(
                "timestampdiff",
                &arena,
                expr_i64,
                &[unit_h, dt1, dt2],
                &chunk
            ),
            3
        );

        let unit_ms = literal_string(&mut arena, "millisecond");
        let dt_ms1 = literal_string(&mut arena, "2020-01-01 00:00:00");
        let dt_ms2 = literal_string(&mut arena, "2020-01-01 00:00:00.250000");
        assert_eq!(
            eval_i64(
                "timestampdiff",
                &arena,
                expr_i64,
                &[unit_ms, dt_ms1, dt_ms2],
                &chunk
            ),
            250
        );

        let left = literal_string(&mut arena, "2020-01-01 00:00:03");
        let right = literal_string(&mut arena, "2020-01-01 00:00:01");
        assert_eq!(
            eval_i64(
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
    fn test_convert_tz() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_ts = typed_null(&mut arena, DataType::Timestamp(TimeUnit::Microsecond, None));
        let dt = literal_string(&mut arena, "2020-01-01 00:00:00");
        let from = literal_string(&mut arena, "+00:00");
        let to = literal_string(&mut arena, "+08:00");
        assert_eq!(
            eval_ts("convert_tz", &arena, expr_ts, &[dt, from, to], &chunk),
            dt_micros("2020-01-01 08:00:00")
        );
    }

    #[test]
    fn test_unix_timestamp_and_from_unixtime() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_ts = typed_null(&mut arena, DataType::Timestamp(TimeUnit::Microsecond, None));
        let expr_i64 = typed_null(&mut arena, DataType::Int64);

        let micros = literal_i64(&mut arena, 1_000_000);
        assert_eq!(
            eval_ts("from_unixtime", &arena, expr_ts, &[micros], &chunk),
            dt_micros("1970-01-01 00:00:01")
        );
        let millis = literal_i64(&mut arena, 1_000);
        assert_eq!(
            eval_ts("from_unixtime_ms", &arena, expr_ts, &[millis], &chunk),
            dt_micros("1970-01-01 00:00:01")
        );

        let dt = literal_string(&mut arena, "1970-01-01 00:00:01");
        assert_eq!(
            eval_i64("unix_timestamp", &arena, expr_i64, &[dt], &chunk),
            1
        );

        let secs = literal_i64(&mut arena, 5 * 3600);
        let expected_hour = Local
            .timestamp_opt(5 * 3600, 0)
            .single()
            .map(|dt| dt.hour() as i64)
            .unwrap();
        assert_eq!(
            eval_i64("hour_from_unixtime", &arena, expr_i64, &[secs], &chunk),
            expected_hour
        );
    }

    #[test]
    fn test_to_days_and_from_days() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_i64 = typed_null(&mut arena, DataType::Int64);
        let expr_date = typed_null(&mut arena, DataType::Date32);

        let date = NaiveDate::from_ymd_opt(1970, 1, 2).unwrap();
        let days = (julian_from_date(date) - BC_EPOCH_JULIAN) as i64;
        let date_str = literal_string(&mut arena, "1970-01-02");
        assert_eq!(
            eval_i64("to_days", &arena, expr_i64, &[date_str], &chunk),
            days
        );

        let days_lit = literal_i64(&mut arena, days);
        let expected_date = date32_from_ymd(1970, 1, 2);
        assert_eq!(
            eval_date32("from_days", &arena, expr_date, &[days_lit], &chunk),
            expected_date
        );
    }

    #[test]
    fn test_last_day_makedate_next_previous() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_date = typed_null(&mut arena, DataType::Date32);

        let d = literal_string(&mut arena, "2020-02-10");
        assert_eq!(
            eval_date32("last_day", &arena, expr_date, &[d], &chunk),
            date32_from_ymd(2020, 2, 29)
        );
        let d_quarter = literal_string(&mut arena, "2020-02-10");
        let quarter = literal_string(&mut arena, "quarter");
        assert_eq!(
            eval_date32("last_day", &arena, expr_date, &[d_quarter, quarter], &chunk),
            date32_from_ymd(2020, 3, 31)
        );
        let d_year = literal_string(&mut arena, "2020-02-10");
        let year_unit = literal_string(&mut arena, "year");
        assert_eq!(
            eval_date32("last_day", &arena, expr_date, &[d_year, year_unit], &chunk),
            date32_from_ymd(2020, 12, 31)
        );

        let year = literal_i64(&mut arena, 2020);
        let day = literal_i64(&mut arena, 32);
        assert_eq!(
            eval_date32("makedate", &arena, expr_date, &[year, day], &chunk),
            date32_from_ymd(2020, 2, 1)
        );

        let base = literal_string(&mut arena, "2020-01-02");
        let friday = literal_string(&mut arena, "Fr");
        assert_eq!(
            eval_date32("next_day", &arena, expr_date, &[base, friday], &chunk),
            date32_from_ymd(2020, 1, 3)
        );
        let invalid = literal_string(&mut arena, "mon");
        let next_err = eval_date_function("next_day", &arena, expr_date, &[base, invalid], &chunk)
            .unwrap_err();
        assert!(next_err.contains("mon not supported in next_day dow_string backend"));
        let prev = literal_string(&mut arena, "2020-01-02");
        let friday2 = literal_string(&mut arena, "Fr");
        assert_eq!(
            eval_date32("previous_day", &arena, expr_date, &[prev, friday2], &chunk),
            date32_from_ymd(2019, 12, 27)
        );
        let monday2 = literal_string(&mut arena, "mon");
        let prev_err =
            eval_date_function("previous_day", &arena, expr_date, &[prev, monday2], &chunk)
                .unwrap_err();
        assert!(prev_err.contains("mon not supported in previous_day dow_string backend"));
    }

    #[test]
    fn test_numeric_coercion_for_makedate_and_microseconds() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_date = typed_null(&mut arena, DataType::Date32);
        let expr_ts = typed_null(&mut arena, DataType::Timestamp(TimeUnit::Microsecond, None));

        let year_str = literal_string(&mut arena, "2024");
        let day_float = literal_f64(&mut arena, 60.9);
        assert_eq!(
            eval_date32(
                "makedate",
                &arena,
                expr_date,
                &[year_str, day_float],
                &chunk
            ),
            date32_from_ymd(2024, 2, 29)
        );

        let base_dt = literal_string(&mut arena, "2024-02-29 12:00:00");
        let micros_str = literal_string(&mut arena, "123456");
        assert_eq!(
            eval_ts(
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
        let chunk = chunk_len_1();
        let expr_i64 = typed_null(&mut arena, DataType::Int64);

        let neg = literal_i64(&mut arena, -1);
        let arr =
            eval_date_function("hour_from_unixtime", &arena, expr_i64, &[neg], &chunk).unwrap();
        let arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(arr.is_null(0));

        let overflow = literal_i64(&mut arena, 253_402_243_200);
        let arr = eval_date_function("hour_from_unixtime", &arena, expr_i64, &[overflow], &chunk)
            .unwrap();
        let arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(arr.is_null(0));
    }

    #[test]
    fn test_next_previous_day_invalid_dow_string() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_date = typed_null(&mut arena, DataType::Date32);

        let base = literal_string(&mut arena, "2020-01-02");
        let invalid = literal_string(&mut arena, "mon");
        let err = eval_date_function("next_day", &arena, expr_date, &[base, invalid], &chunk)
            .expect_err("next_day should reject lowercase dow string");
        assert!(err.contains("mon not supported in next_day dow_string backend"));

        let base_prev = literal_string(&mut arena, "2020-01-02");
        let invalid_prev = literal_string(&mut arena, "monday");
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
        let chunk = chunk_len_1();
        let expr_date = typed_null(&mut arena, DataType::Date32);

        let null_date = typed_null(&mut arena, DataType::Utf8);
        let invalid = literal_string(&mut arena, "xxx");
        let arr = eval_date_function("next_day", &arena, expr_date, &[null_date, invalid], &chunk)
            .expect("null date should short-circuit to NULL without validating day token");
        let arr = arr.as_any().downcast_ref::<Date32Array>().unwrap();
        assert!(arr.is_null(0));
    }

    #[test]
    fn test_time_to_sec_and_sec_to_time_and_timediff() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_i64 = typed_null(&mut arena, DataType::Int64);
        let expr_str = typed_null(&mut arena, DataType::Utf8);

        let dt = literal_string(&mut arena, "01:01:01");
        assert_eq!(
            eval_i64("time_to_sec", &arena, expr_i64, &[dt], &chunk),
            3661
        );

        let secs = literal_i64(&mut arena, 3661);
        assert_eq!(
            eval_str("sec_to_time", &arena, expr_str, &[secs], &chunk),
            "01:01:01"
        );

        let t1 = literal_string(&mut arena, "2020-01-02 03:00:00");
        let t2 = literal_string(&mut arena, "2020-01-02 01:00:00");
        assert_eq!(
            eval_str("timediff", &arena, expr_str, &[t1, t2], &chunk),
            "02:00:00"
        );
    }

    #[test]
    fn test_trunc_and_slice_and_alignment() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_ts = typed_null(&mut arena, DataType::Timestamp(TimeUnit::Microsecond, None));
        let expr_date = typed_null(&mut arena, DataType::Date32);
        let unit_month = literal_string(&mut arena, "month");
        let dt = literal_string(&mut arena, "2020-01-15 12:34:56");
        assert_eq!(
            eval_ts("date_trunc", &arena, expr_ts, &[unit_month, dt], &chunk),
            dt_micros("2020-01-01 00:00:00")
        );
        assert_eq!(
            eval_date32("date_trunc", &arena, expr_date, &[unit_month, dt], &chunk),
            date32_from_ymd(2020, 1, 1)
        );
        let unit_millisecond = literal_string(&mut arena, "millisecond");
        let dt_ms = literal_string(&mut arena, "2020-01-15 12:34:56.123456");
        let expected_ms =
            NaiveDateTime::parse_from_str("2020-01-15 12:34:56.123000", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap()
                .and_utc()
                .timestamp_micros();
        assert_eq!(
            eval_ts(
                "date_trunc",
                &arena,
                expr_ts,
                &[unit_millisecond, dt_ms],
                &chunk
            ),
            expected_ms
        );
        let invalid_unit = literal_string(&mut arena, "foo");
        let invalid_dt = literal_string(&mut arena, "2020-01-15 12:34:56");
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
            eval_ts("date_floor", &arena, expr_ts, &[unit_month, dt], &chunk),
            dt_micros("2020-01-01 00:00:00")
        );

        let unit_day = literal_string(&mut arena, "day");
        assert_eq!(
            eval_ts(
                "alignment_timestamp",
                &arena,
                expr_ts,
                &[unit_day, dt],
                &chunk
            ),
            dt_micros("2020-01-15 00:00:00")
        );

        let interval = literal_i64(&mut arena, 1);
        let unit_hour = literal_string(&mut arena, "hour");
        let dt2 = literal_string(&mut arena, "2020-01-02 03:04:05");
        assert_eq!(
            eval_ts(
                "time_slice",
                &arena,
                expr_ts,
                &[dt2, interval, unit_hour],
                &chunk
            ),
            dt_micros("2020-01-02 03:00:00")
        );
        let dt3 = literal_string(&mut arena, "2020-01-02 03:04:05");
        assert_eq!(
            eval_ts(
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
        let chunk = chunk_len_1();
        let expr_str = typed_null(&mut arena, DataType::Utf8);
        let s = literal_string(&mut arena, "2020-01-02");
        assert_eq!(
            eval_str("substitute", &arena, expr_str, &[s], &chunk),
            "2020-01-02"
        );
    }

    #[test]
    fn test_current_and_utc() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_ts = typed_null(&mut arena, DataType::Timestamp(TimeUnit::Microsecond, None));
        let expr_date = typed_null(&mut arena, DataType::Date32);

        let now_local = Local::now().naive_local();
        let now_local_ts = now_local.and_utc().timestamp_micros();
        let actual = eval_ts("current_timestamp", &arena, expr_ts, &[], &chunk);
        assert!((actual - now_local_ts).abs() < 5_000_000);

        let today = now_local.date();
        let actual_date = eval_date32("current_date", &arena, expr_date, &[], &chunk);
        assert_eq!(actual_date, naive_to_date32(today));
        let actual_curdate = eval_date32("curdate", &arena, expr_date, &[], &chunk);
        assert_eq!(actual_curdate, naive_to_date32(today));

        let now_time = now_local.time();
        let base = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let expected_time_ts = base.and_time(now_time).and_utc().timestamp_micros();
        let actual_time = eval_ts("current_time", &arena, expr_ts, &[], &chunk);
        assert!((actual_time - expected_time_ts).abs() < 5_000_000);
        let actual_curtime = eval_ts("curtime", &arena, expr_ts, &[], &chunk);
        assert!((actual_curtime - expected_time_ts).abs() < 5_000_000);

        let actual_now = eval_ts("now", &arena, expr_ts, &[], &chunk);
        assert!((actual_now - now_local_ts).abs() < 5_000_000);
        let actual_localtime = eval_ts("localtime", &arena, expr_ts, &[], &chunk);
        assert!((actual_localtime - now_local_ts).abs() < 5_000_000);
        let actual_localts = eval_ts("localtimestamp", &arena, expr_ts, &[], &chunk);
        assert!((actual_localts - now_local_ts).abs() < 5_000_000);

        let now_utc = Utc::now().naive_utc();
        let now_utc_ts = now_utc.and_utc().timestamp_micros();
        let actual_utc = eval_ts("utc_timestamp", &arena, expr_ts, &[], &chunk);
        assert!((actual_utc - now_utc_ts).abs() < 5_000_000);

        let utc_time = now_utc.time();
        let expected_utc_time = base.and_time(utc_time).and_utc().timestamp_micros();
        let actual_utc_time = eval_ts("utc_time", &arena, expr_ts, &[], &chunk);
        assert!((actual_utc_time - expected_utc_time).abs() < 5_000_000);
    }
}
