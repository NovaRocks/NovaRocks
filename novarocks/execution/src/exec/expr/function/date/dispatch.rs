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
            if *canonical == "hour_from_unixtime" {
                crate::exec::expr::function::FunctionKind::HourFromUnixtime
            } else {
                crate::exec::expr::function::FunctionKind::Date(canonical)
            },
        );
    }
}

pub fn metadata(name: &str) -> Option<FunctionMeta> {
    DATE_METADATA.iter().find(|m| m.name == name).copied()
}

pub fn eval_date_function<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    name: &str,
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let canonical = DATE_FUNCTIONS
        .iter()
        .find_map(|(alias, target)| (*alias == name).then_some(*target))
        .unwrap_or(name);

    match canonical {
        "add_months" => Ok((super::add_months::eval_add_months(arena, expr, args, chunk))?),
        "date_add" => Ok((super::date_add::eval_date_add(arena, expr, args, chunk))?),
        "date_diff" => Ok((super::datediff::eval_date_diff(arena, expr, args, chunk))?),
        "alignment_timestamp" => {
            Ok((super::date_trunc::eval_alignment_timestamp(arena, expr, args, chunk))?)
        }
        "convert_tz" => Ok((super::convert_tz::eval_convert_tz(arena, expr, args, chunk))?),
        "current_date" => Ok((super::current_date::eval_current_date(arena, expr, args, chunk))?),
        "current_time" => Ok((super::current_time::eval_current_time(arena, expr, args, chunk))?),
        "current_timestamp" => {
            Ok((super::current_timestamp::eval_current_timestamp(arena, expr, args, chunk))?)
        }
        "date" => Ok((super::date::eval_date(arena, expr, args, chunk))?),
        "date_floor" => Ok((super::date_trunc::eval_date_floor(arena, expr, args, chunk))?),
        "date_format" => Ok((super::date_format::eval_date_format(arena, expr, args, chunk))?),
        "date_slice" => Ok((super::time_slice::eval_date_slice(arena, expr, args, chunk))?),
        "date_sub" => Ok((super::date_add::eval_date_sub(arena, expr, args, chunk))?),
        "date_trunc" => Ok((super::date_trunc::eval_date_trunc(arena, expr, args, chunk))?),
        "datediff" => Ok((super::datediff::eval_datediff(arena, expr, args, chunk))?),
        "day" => Ok((super::date_parts::eval_day(arena, expr, args, chunk))?),
        "dayname" => Ok((super::date_parts::eval_dayname(arena, expr, args, chunk))?),
        "dayofweek" => Ok((super::date_parts::eval_dayofweek(arena, expr, args, chunk))?),
        "dayofweek_iso" => Ok((super::date_parts::eval_dayofweek_iso(arena, expr, args, chunk))?),
        "dayofyear" => Ok((super::date_parts::eval_dayofyear(arena, expr, args, chunk))?),
        "days_add" => Ok((super::date_add::eval_days_add(arena, expr, args, chunk))?),
        "days_diff" => Ok((super::datediff::eval_days_diff(arena, expr, args, chunk))?),
        "days_sub" => Ok((super::date_add::eval_days_sub(arena, expr, args, chunk))?),
        "from_days" => Ok((super::from_days::eval_from_days(arena, expr, args, chunk))?),
        "from_unixtime" => {
            Ok((super::from_unixtime::eval_from_unixtime(arena, expr, args, chunk))?)
        }
        "from_unixtime_ms" => {
            Ok((super::from_unixtime::eval_from_unixtime_ms(arena, expr, args, chunk))?)
        }
        "hour" => Ok((super::date_parts::eval_hour(arena, expr, args, chunk))?),
        "hour_from_unixtime" => {
            Ok((super::hour_from_unixtime::eval_hour_from_unixtime(arena, expr, args, chunk))?)
        }
        "hours_add" => Ok((super::seconds_add::eval_hours_add(arena, expr, args, chunk))?),
        "hours_diff" => Ok((super::datediff::eval_hours_diff(arena, expr, args, chunk))?),
        "hours_sub" => Ok((super::seconds_add::eval_hours_sub(arena, expr, args, chunk))?),
        "jodatime_format" => {
            Ok((super::date_format::eval_jodatime_format(arena, expr, args, chunk))?)
        }
        "last_day" => Ok((super::last_day::eval_last_day(arena, expr, args, chunk))?),
        "makedate" => Ok((super::makedate::eval_makedate(arena, expr, args, chunk))?),
        "microseconds_add" => {
            Ok((super::seconds_add::eval_microseconds_add(arena, expr, args, chunk))?)
        }
        "microseconds_sub" => {
            Ok((super::seconds_add::eval_microseconds_sub(arena, expr, args, chunk))?)
        }
        "milliseconds_add" => {
            Ok((super::seconds_add::eval_milliseconds_add(arena, expr, args, chunk))?)
        }
        "milliseconds_diff" => {
            Ok((super::datediff::eval_milliseconds_diff(arena, expr, args, chunk))?)
        }
        "milliseconds_sub" => {
            Ok((super::seconds_add::eval_milliseconds_sub(arena, expr, args, chunk))?)
        }
        "minute" => Ok((super::date_parts::eval_minute(arena, expr, args, chunk))?),
        "minutes_add" => Ok((super::seconds_add::eval_minutes_add(arena, expr, args, chunk))?),
        "minutes_diff" => Ok((super::datediff::eval_minutes_diff(arena, expr, args, chunk))?),
        "minutes_sub" => Ok((super::seconds_add::eval_minutes_sub(arena, expr, args, chunk))?),
        "month" => Ok((super::date_parts::eval_month(arena, expr, args, chunk))?),
        "monthname" => Ok((super::date_parts::eval_monthname(arena, expr, args, chunk))?),
        "months_add" => Ok((super::add_months::eval_months_add(arena, expr, args, chunk))?),
        "months_diff" => Ok((super::datediff::eval_months_diff(arena, expr, args, chunk))?),
        "months_sub" => Ok((super::add_months::eval_months_sub(arena, expr, args, chunk))?),
        "next_day" => Ok((super::next_day::eval_next_day(arena, expr, args, chunk))?),
        "previous_day" => Ok((super::next_day::eval_previous_day(arena, expr, args, chunk))?),
        "quarter" => Ok((super::date_parts::eval_quarter(arena, expr, args, chunk))?),
        "quarters_add" => Ok((super::add_months::eval_quarters_add(arena, expr, args, chunk))?),
        "quarters_sub" => Ok((super::add_months::eval_quarters_sub(arena, expr, args, chunk))?),
        "sec_to_time" => Ok((super::sec_to_time::eval_sec_to_time(arena, expr, args, chunk))?),
        "second" => Ok((super::date_parts::eval_second(arena, expr, args, chunk))?),
        "seconds_add" => Ok((super::seconds_add::eval_seconds_add(arena, expr, args, chunk))?),
        "seconds_diff" => Ok((super::datediff::eval_seconds_diff(arena, expr, args, chunk))?),
        "seconds_sub" => Ok((super::seconds_add::eval_seconds_sub(arena, expr, args, chunk))?),
        "str_to_jodatime" => Ok((super::date::eval_str_to_jodatime(arena, expr, args, chunk))?),
        "str_to_date" => Ok((super::str_to_date::eval_str_to_date(arena, expr, args, chunk))?),
        "substitute" => Ok((super::substitute::eval_substitute(arena, expr, args, chunk))?),
        "time_format" => Ok((super::time_format::eval_time_format(arena, expr, args, chunk))?),
        "time_slice" => Ok((super::time_slice::eval_time_slice(arena, expr, args, chunk))?),
        "time_to_sec" => Ok((super::time_to_sec::eval_time_to_sec(arena, expr, args, chunk))?),
        "timediff" => Ok((super::timediff::eval_timediff(arena, expr, args, chunk))?),
        "timestamp" => Ok((super::date::eval_timestamp(arena, expr, args, chunk))?),
        "timestampadd" => Ok((super::timestampadd::eval_timestampadd(arena, expr, args, chunk))?),
        "timestampdiff" => {
            Ok((super::timestampdiff::eval_timestampdiff(arena, expr, args, chunk))?)
        }
        "to_date" => Ok((super::date::eval_to_date(arena, expr, args, chunk))?),
        "to_datetime" => Ok((super::date::eval_to_datetime(arena, expr, args, chunk))?),
        "to_datetime_ntz" => Ok((super::date::eval_to_datetime_ntz(arena, expr, args, chunk))?),
        "to_days" => Ok((super::to_days::eval_to_days(arena, expr, args, chunk))?),
        "to_iso8601" => Ok((super::to_iso8601::eval_to_iso8601(arena, expr, args, chunk))?),
        "to_tera_date" => Ok((super::date::eval_to_tera_date(arena, expr, args, chunk))?),
        "to_tera_timestamp" => Ok((super::date::eval_to_tera_timestamp(arena, expr, args, chunk))?),
        "unix_timestamp" => {
            Ok((super::unix_timestamp::eval_unix_timestamp(arena, expr, args, chunk))?)
        }
        "utc_time" => Ok((super::current_time::eval_utc_time(arena, expr, args, chunk))?),
        "utc_timestamp" => {
            Ok((super::current_timestamp::eval_utc_timestamp(arena, expr, args, chunk))?)
        }
        "week" => Ok((super::date_parts::eval_week(arena, expr, args, chunk))?),
        "weekday" => Ok((super::date_parts::eval_weekday(arena, expr, args, chunk))?),
        "weeks_add" => Ok((super::date_add::eval_weeks_add(arena, expr, args, chunk))?),
        "weeks_diff" => Ok((super::datediff::eval_weeks_diff(arena, expr, args, chunk))?),
        "weeks_sub" => Ok((super::date_add::eval_weeks_sub(arena, expr, args, chunk))?),
        "year" => Ok((super::date_parts::eval_year(arena, expr, args, chunk))?),
        "yearweek" => Ok((super::date_parts::eval_yearweek(arena, expr, args, chunk))?),
        "years_add" => Ok((super::add_months::eval_years_add(arena, expr, args, chunk))?),
        "years_diff" => Ok((super::datediff::eval_years_diff(arena, expr, args, chunk))?),
        "years_sub" => Ok((super::add_months::eval_years_sub(arena, expr, args, chunk))?),
        "quarters_diff" => Ok((super::datediff::eval_quarters_diff(arena, expr, args, chunk))?),
        other => Err((format!("unsupported date function: {}", other)).into()),
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
        max_args: 3,
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
        min_args: 2,
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
