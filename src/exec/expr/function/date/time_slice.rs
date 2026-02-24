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
use arrow::array::{Array, ArrayRef, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime};
use std::sync::Arc;

#[derive(Copy, Clone, Debug)]
enum TimeSliceUnit {
    Year,
    Quarter,
    Month,
    Week,
    Day,
    Hour,
    Minute,
    Second,
    Millisecond,
    Microsecond,
}

impl TimeSliceUnit {
    fn parse(raw: &str) -> Result<Self, String> {
        match raw.to_ascii_lowercase().as_str() {
            "year" | "years" => Ok(Self::Year),
            "quarter" | "quarters" => Ok(Self::Quarter),
            "month" | "months" => Ok(Self::Month),
            "week" | "weeks" => Ok(Self::Week),
            "day" | "days" => Ok(Self::Day),
            "hour" | "hours" => Ok(Self::Hour),
            "minute" | "minutes" => Ok(Self::Minute),
            "second" | "seconds" => Ok(Self::Second),
            "millisecond" | "milliseconds" => Ok(Self::Millisecond),
            "microsecond" | "microseconds" => Ok(Self::Microsecond),
            other => Err(format!("time_slice unsupported unit: {}", other)),
        }
    }

    fn index(self, dt: NaiveDateTime, start: NaiveDateTime) -> i64 {
        let delta = dt.signed_duration_since(start);
        match self {
            Self::Year => (dt.year() - start.year()) as i64,
            Self::Quarter => ((dt.year() - start.year()) as i64) * 4 + (dt.month0() / 3) as i64,
            Self::Month => ((dt.year() - start.year()) as i64) * 12 + dt.month0() as i64,
            Self::Week => delta.num_days() / 7,
            Self::Day => delta.num_days(),
            Self::Hour => delta.num_hours(),
            Self::Minute => delta.num_minutes(),
            Self::Second => delta.num_seconds(),
            Self::Millisecond => delta.num_milliseconds(),
            Self::Microsecond => delta.num_microseconds().unwrap_or(i64::MAX),
        }
    }

    fn sliced_datetime(
        self,
        start: NaiveDateTime,
        epoch: i64,
        interval: i64,
    ) -> Option<NaiveDateTime> {
        match self {
            Self::Year => {
                let delta_years = epoch.checked_mul(interval)?;
                let year = (start.year() as i64).checked_add(delta_years)?;
                if year < i32::MIN as i64 || year > i32::MAX as i64 {
                    return None;
                }
                start.date().with_year(year as i32)?.and_hms_opt(0, 0, 0)
            }
            Self::Quarter => {
                let months = epoch.checked_mul(interval)?.checked_mul(3)?;
                if months < i32::MIN as i64 || months > i32::MAX as i64 {
                    return None;
                }
                Some(super::add_months::add_months_to_datetime(
                    start,
                    months as i32,
                ))
            }
            Self::Month => {
                let months = epoch.checked_mul(interval)?;
                if months < i32::MIN as i64 || months > i32::MAX as i64 {
                    return None;
                }
                Some(super::add_months::add_months_to_datetime(
                    start,
                    months as i32,
                ))
            }
            Self::Week => Some(start + Duration::weeks(epoch.checked_mul(interval)?)),
            Self::Day => Some(start + Duration::days(epoch.checked_mul(interval)?)),
            Self::Hour => Some(start + Duration::hours(epoch.checked_mul(interval)?)),
            Self::Minute => Some(start + Duration::minutes(epoch.checked_mul(interval)?)),
            Self::Second => Some(start + Duration::seconds(epoch.checked_mul(interval)?)),
            Self::Millisecond => Some(start + Duration::milliseconds(epoch.checked_mul(interval)?)),
            Self::Microsecond => Some(start + Duration::microseconds(epoch.checked_mul(interval)?)),
        }
    }
}

fn i64_at(values: &[Option<i64>], row: usize, len: usize) -> Option<i64> {
    let idx = if values.len() == 1 && len > 1 { 0 } else { row };
    values.get(idx).copied().flatten()
}

fn str_at<'a>(arr: &'a StringArray, row: usize, len: usize) -> Option<&'a str> {
    let idx = if arr.len() == 1 && len > 1 { 0 } else { row };
    if idx >= arr.len() || arr.is_null(idx) {
        None
    } else {
        Some(arr.value(idx))
    }
}

#[inline]
fn eval_time_slice_inner(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    // time_slice(datetime, interval, unit, boundary?)
    let dt_arr = arena.eval(args[0], chunk)?;
    let interval_arr = arena.eval(args[1], chunk)?;
    let unit_arr = arena.eval(args[2], chunk)?;
    let boundary_arr = if args.len() == 4 {
        Some(arena.eval(args[3], chunk)?)
    } else {
        None
    };
    let dts = extract_datetime_array(&dt_arr)?;
    let interval_values = extract_i64_array(&interval_arr, "time_slice")
        .map_err(|_| "time_slice expects int interval".to_string())?;
    let unit_arr = unit_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "time_slice expects unit string".to_string())?;
    let boundary_arr = boundary_arr
        .as_ref()
        .and_then(|a| a.as_any().downcast_ref::<StringArray>());

    let output_type = arena
        .data_type(expr)
        .cloned()
        .unwrap_or(DataType::Timestamp(TimeUnit::Microsecond, None));
    let mut out = Vec::with_capacity(dts.len());
    let start = NaiveDate::from_ymd_opt(1, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    for i in 0..dts.len() {
        let Some(interval) = i64_at(&interval_values, i, dts.len()) else {
            out.push(None);
            continue;
        };
        let Some(unit_str) = str_at(unit_arr, i, dts.len()) else {
            out.push(None);
            continue;
        };
        let dt = match dts[i] {
            Some(v) => v,
            None => {
                out.push(None);
                continue;
            }
        };
        if dt < start {
            return Err("time used with time_slice can't before 0001-01-01 00:00:00".to_string());
        }
        if interval <= 0 {
            out.push(None);
            continue;
        }
        let unit = TimeSliceUnit::parse(unit_str)?;
        let boundary = boundary_arr
            .and_then(|b| str_at(b, i, dts.len()))
            .unwrap_or("floor");
        let use_ceil = match boundary.to_ascii_lowercase().as_str() {
            "floor" => false,
            "ceil" => true,
            other => {
                return Err(format!(
                    "time_slice expects boundary floor/ceil, got {}",
                    other
                ));
            }
        };
        let duration = unit.index(dt, start);
        let mut epoch = duration / interval;
        if use_ceil {
            epoch += 1;
        }
        let Some(sliced) = unit.sliced_datetime(start, epoch, interval) else {
            out.push(None);
            continue;
        };
        if !(1..=9999).contains(&sliced.year()) {
            out.push(None);
            continue;
        }
        out.push(to_timestamp_value(sliced, &output_type).ok());
    }
    Ok(Arc::new(TimestampMicrosecondArray::from(out)) as ArrayRef)
}

pub fn eval_time_slice(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_time_slice_inner(arena, expr, args, chunk)
}

pub fn eval_date_slice(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_time_slice_inner(arena, expr, args, chunk)
}

#[cfg(test)]
mod tests {
    use crate::exec::expr::function::date::test_utils::assert_date_function_logic;

    #[test]
    fn test_time_slice_logic() {
        assert_date_function_logic("time_slice");
    }

    #[test]
    fn test_date_slice_logic() {
        assert_date_function_logic("date_slice");
    }
}
