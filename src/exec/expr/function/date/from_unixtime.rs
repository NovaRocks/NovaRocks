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
    extract_i64_array, naive_to_date32, parse_date, parse_datetime, to_timestamp_value,
};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{
    Array, ArrayRef, Date32Array, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{DateTime, FixedOffset, Local, NaiveDateTime, Utc};
use chrono_tz::Tz;
use std::str::FromStr;
use std::sync::Arc;

const DEFAULT_FROM_UNIXTIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S";
const MAX_FROM_UNIXTIME_SECONDS: i64 = 253_402_243_199;
const MAX_FROM_UNIXTIME_FORMAT_LEN: usize = 128;

#[derive(Clone, Copy)]
enum TimeZoneSpec {
    Local,
    Fixed(FixedOffset),
    Named(Tz),
}

fn parse_fixed_offset(s: &str) -> Option<FixedOffset> {
    if let Ok(offset) = FixedOffset::from_str(s) {
        return Some(offset);
    }

    let sign = match s.as_bytes().first().copied() {
        Some(b'+') => 1,
        Some(b'-') => -1,
        _ => return None,
    };
    let rest = &s[1..];
    let (hours, minutes) = rest.split_once(':')?;
    let hours = hours.parse::<i32>().ok()?;
    let minutes = minutes.parse::<i32>().ok()?;
    if !(0..=23).contains(&hours) || !(0..=59).contains(&minutes) {
        return None;
    }
    FixedOffset::east_opt(sign * (hours * 3600 + minutes * 60))
}

fn parse_tz(s: &str) -> Option<TimeZoneSpec> {
    if s.eq_ignore_ascii_case("local") {
        return Some(TimeZoneSpec::Local);
    }
    if s.eq_ignore_ascii_case("utc") {
        return FixedOffset::east_opt(0).map(TimeZoneSpec::Fixed);
    }
    if let Some(offset) = parse_fixed_offset(s) {
        return Some(TimeZoneSpec::Fixed(offset));
    }
    Tz::from_str(s).ok().map(TimeZoneSpec::Named)
}

fn normalize_mysql_from_unixtime_format(fmt: &str) -> Option<String> {
    let mut out = String::with_capacity(fmt.len());
    let mut chars = fmt.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '%' {
            if ch.is_ascii_alphabetic() {
                return None;
            }
            out.push(ch);
            continue;
        }

        let spec = chars.next()?;
        match spec {
            'Y' => out.push_str("%Y"),
            'm' => out.push_str("%m"),
            'd' => out.push_str("%d"),
            'H' => out.push_str("%H"),
            'i' => out.push_str("%M"),
            's' | 'S' => out.push_str("%S"),
            '%' => out.push('%'),
            _ => return None,
        }
    }
    Some(out)
}

fn normalize_from_unixtime_format(fmt: &str) -> Option<String> {
    if fmt.is_empty() || fmt.len() > MAX_FROM_UNIXTIME_FORMAT_LEN {
        return None;
    }
    match fmt {
        "yyyy-MM-dd HH:mm:ss" => Some(DEFAULT_FROM_UNIXTIME_FORMAT.to_string()),
        "yyyy-MM-dd" => Some("%Y-%m-%d".to_string()),
        "yyyyMMdd" => Some("%Y%m%d".to_string()),
        _ => normalize_mysql_from_unixtime_format(fmt),
    }
}

fn epoch_value_to_datetime(
    value: i64,
    units_per_second: i64,
    timezone: TimeZoneSpec,
) -> Option<NaiveDateTime> {
    if value < 0 || units_per_second <= 0 || 1_000_000_000 % units_per_second != 0 {
        return None;
    }

    let seconds = value.div_euclid(units_per_second);
    if !(0..=MAX_FROM_UNIXTIME_SECONDS).contains(&seconds) {
        return None;
    }

    let remainder = value.rem_euclid(units_per_second) as u32;
    let nanos = remainder.checked_mul((1_000_000_000 / units_per_second) as u32)?;
    let dt_utc = DateTime::<Utc>::from_timestamp(seconds, nanos)?;
    Some(match timezone {
        TimeZoneSpec::Local => dt_utc.with_timezone(&Local).naive_local(),
        TimeZoneSpec::Fixed(offset) => dt_utc.with_timezone(&offset).naive_local(),
        TimeZoneSpec::Named(tz) => dt_utc.with_timezone(&tz).naive_local(),
    })
}

fn build_timestamp_array(
    values: Vec<Option<i64>>,
    output_type: &DataType,
) -> Result<ArrayRef, String> {
    let (unit, tz) = match output_type {
        DataType::Timestamp(unit, tz) => (unit.clone(), tz.as_deref().map(|s| s.to_string())),
        other => {
            return Err(format!(
                "from_unixtime unsupported output type: {:?}",
                other
            ));
        }
    };

    let array: ArrayRef = match unit {
        TimeUnit::Second => {
            let array = TimestampSecondArray::from(values);
            if let Some(tz) = tz {
                Arc::new(array.with_timezone(tz))
            } else {
                Arc::new(array)
            }
        }
        TimeUnit::Millisecond => {
            let array = TimestampMillisecondArray::from(values);
            if let Some(tz) = tz {
                Arc::new(array.with_timezone(tz))
            } else {
                Arc::new(array)
            }
        }
        TimeUnit::Microsecond => {
            let array = TimestampMicrosecondArray::from(values);
            if let Some(tz) = tz {
                Arc::new(array.with_timezone(tz))
            } else {
                Arc::new(array)
            }
        }
        TimeUnit::Nanosecond => {
            let array = TimestampNanosecondArray::from(values);
            if let Some(tz) = tz {
                Arc::new(array.with_timezone(tz))
            } else {
                Arc::new(array)
            }
        }
    };
    Ok(array)
}

fn resolve_output_type(arena: &ExprArena, expr: ExprId, has_format_arg: bool) -> DataType {
    match arena.data_type(expr).cloned() {
        Some(DataType::Null) | None if has_format_arg => DataType::Utf8,
        Some(DataType::Null) | None => DataType::Timestamp(TimeUnit::Microsecond, None),
        Some(data_type) => data_type,
    }
}

fn default_time_zone(arena: &ExprArena) -> TimeZoneSpec {
    arena
        .session_time_zone()
        .and_then(parse_tz)
        .unwrap_or(TimeZoneSpec::Local)
}

fn eval_from_unixtime_inner(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    units_per_second: i64,
) -> Result<ArrayRef, String> {
    let values = extract_i64_array(&arena.eval(args[0], chunk)?, "from_unixtime")?;
    let has_format_arg = args.len() > 1;
    let output_type = resolve_output_type(arena, expr, has_format_arg);

    if !has_format_arg {
        let default_tz = default_time_zone(arena);
        let datetimes: Vec<Option<NaiveDateTime>> = values
            .into_iter()
            .map(|value| {
                value.and_then(|v| epoch_value_to_datetime(v, units_per_second, default_tz))
            })
            .collect();

        return match output_type {
            DataType::Utf8 => {
                let out: Vec<Option<String>> = datetimes
                    .into_iter()
                    .map(|dt| dt.map(|v| v.format(DEFAULT_FROM_UNIXTIME_FORMAT).to_string()))
                    .collect();
                Ok(Arc::new(StringArray::from(out)) as ArrayRef)
            }
            DataType::Date32 => {
                let out: Vec<Option<i32>> = datetimes
                    .into_iter()
                    .map(|dt| dt.map(|v| naive_to_date32(v.date())))
                    .collect();
                Ok(Arc::new(Date32Array::from(out)) as ArrayRef)
            }
            DataType::Timestamp(_, _) => {
                let out: Vec<Option<i64>> = datetimes
                    .into_iter()
                    .map(|dt| dt.and_then(|v| to_timestamp_value(v, &output_type).ok()))
                    .collect();
                build_timestamp_array(out, &output_type)
            }
            other => Err(format!(
                "from_unixtime unsupported output type: {:?}",
                other
            )),
        };
    }

    let format_arr = arena.eval(args[1], chunk)?;
    let format_arr = format_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "from_unixtime expects string format".to_string())?;
    let timezone_arr = if args.len() > 2 {
        Some(
            arena
                .eval(args[2], chunk)?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "from_unixtime expects string timezone".to_string())?
                .clone(),
        )
    } else {
        None
    };
    let default_tz = default_time_zone(arena);

    let mut formatted = Vec::with_capacity(values.len());
    for i in 0..values.len() {
        let Some(raw_value) = values[i] else {
            formatted.push(None);
            continue;
        };
        if format_arr.is_null(i) {
            formatted.push(None);
            continue;
        }
        let Some(format) = normalize_from_unixtime_format(format_arr.value(i)) else {
            formatted.push(None);
            continue;
        };
        let timezone = if let Some(ref tz_arr) = timezone_arr {
            if tz_arr.is_null(i) {
                formatted.push(None);
                continue;
            }
            let Some(tz) = parse_tz(tz_arr.value(i)) else {
                formatted.push(None);
                continue;
            };
            tz
        } else {
            default_tz
        };
        let Some(dt) = epoch_value_to_datetime(raw_value, units_per_second, timezone) else {
            formatted.push(None);
            continue;
        };
        let rendered = dt.format(&format).to_string();
        if rendered.len() > MAX_FROM_UNIXTIME_FORMAT_LEN {
            formatted.push(None);
            continue;
        }
        formatted.push(Some(rendered));
    }

    match output_type {
        DataType::Utf8 => Ok(Arc::new(StringArray::from(formatted)) as ArrayRef),
        DataType::Date32 => {
            let out: Vec<Option<i32>> = formatted
                .into_iter()
                .map(|value| {
                    value.and_then(|s| {
                        parse_datetime(&s)
                            .map(|dt| naive_to_date32(dt.date()))
                            .or_else(|| parse_date(&s).map(naive_to_date32))
                    })
                })
                .collect();
            Ok(Arc::new(Date32Array::from(out)) as ArrayRef)
        }
        DataType::Timestamp(_, _) => {
            let out: Vec<Option<i64>> = formatted
                .into_iter()
                .map(|value| {
                    value.and_then(|s| {
                        parse_datetime(&s)
                            .or_else(|| parse_date(&s).and_then(|date| date.and_hms_opt(0, 0, 0)))
                            .and_then(|dt| to_timestamp_value(dt, &output_type).ok())
                    })
                })
                .collect();
            build_timestamp_array(out, &output_type)
        }
        other => Err(format!(
            "from_unixtime unsupported output type: {:?}",
            other
        )),
    }
}

#[inline]
pub fn eval_from_unixtime(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_from_unixtime_inner(arena, expr, args, chunk, 1)
}

pub fn eval_from_unixtime_ms(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_from_unixtime_inner(arena, expr, args, chunk, 1_000)
}
