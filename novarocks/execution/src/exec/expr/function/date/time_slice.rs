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
    extract_datetime_array, extract_i64_array, naive_to_date32, to_timestamp_value,
};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{Array, ArrayRef, Date32Array, StringArray, TimestampMicrosecondArray};
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

    fn index(self, dt: NaiveDateTime, start: NaiveDateTime) -> Option<i64> {
        let delta = dt.signed_duration_since(start);
        Some(match self {
            Self::Year => i64::from(dt.year() - start.year()),
            Self::Quarter => ((dt.year() - start.year()) as i64) * 4 + (dt.month0() / 3) as i64,
            Self::Month => ((dt.year() - start.year()) as i64) * 12 + dt.month0() as i64,
            Self::Week => delta.num_days() / 7,
            Self::Day => delta.num_days(),
            Self::Hour => delta.num_hours(),
            Self::Minute => delta.num_minutes(),
            Self::Second => delta.num_seconds(),
            Self::Millisecond => delta.num_milliseconds(),
            Self::Microsecond => delta.num_microseconds()?,
        })
    }

    fn sliced_datetime(
        self,
        start: NaiveDateTime,
        epoch: i64,
        interval: i64,
    ) -> Option<NaiveDateTime> {
        let units = epoch.checked_mul(interval)?;
        match self {
            Self::Year => {
                let year = i32::try_from(i64::from(start.year()).checked_add(units)?).ok()?;
                NaiveDate::from_ymd_opt(year, 1, 1)?.and_hms_opt(0, 0, 0)
            }
            Self::Quarter | Self::Month => {
                let months = if matches!(self, Self::Quarter) {
                    units.checked_mul(3)?
                } else {
                    units
                };
                let year =
                    i32::try_from(i64::from(start.year()).checked_add(months.div_euclid(12))?)
                        .ok()?;
                let month = u32::try_from(months.rem_euclid(12).checked_add(1)?).ok()?;
                NaiveDate::from_ymd_opt(year, month, 1)?.and_hms_opt(0, 0, 0)
            }
            // Checked durations/addition preserve legitimate overflow -> NULL
            // without a chrono panic for large positive INT32 intervals.
            Self::Week => start.checked_add_signed(Duration::try_days(units.checked_mul(7)?)?),
            Self::Day => start.checked_add_signed(Duration::try_days(units)?),
            Self::Hour => start.checked_add_signed(Duration::try_hours(units)?),
            Self::Minute => start.checked_add_signed(Duration::try_minutes(units)?),
            Self::Second => start.checked_add_signed(Duration::try_seconds(units)?),
            Self::Millisecond => start.checked_add_signed(Duration::try_milliseconds(units)?),
            Self::Microsecond => start.checked_add_signed(Duration::microseconds(units)),
        }
    }
}

// A single result vector uses the frozen result's physical width. No temporary
// datetime output vector or per-row type guessing is needed.
enum SliceValues {
    Date(Vec<Option<i32>>),
    Datetime(Vec<Option<i64>>),
}

impl SliceValues {
    fn push(&mut self, value: Option<NaiveDateTime>) -> Result<(), String> {
        match self {
            Self::Date(values) => values.push(value.map(|dt| naive_to_date32(dt.date()))),
            Self::Datetime(values) => values.push(
                value
                    .map(|dt| {
                        to_timestamp_value(dt, &DataType::Timestamp(TimeUnit::Microsecond, None))
                    })
                    .transpose()?,
            ),
        }
        Ok(())
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Date(values) => Arc::new(Date32Array::from(values)),
            Self::Datetime(values) => Arc::new(TimestampMicrosecondArray::from(values)),
        }
    }
}

fn i64_at(values: &[Option<i64>], row: usize, len: usize) -> Option<i64> {
    let idx = if values.len() == 1 && len > 1 { 0 } else { row };
    values.get(idx).copied().flatten()
}

fn str_at(arr: &StringArray, row: usize, len: usize) -> Option<&str> {
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
    date_output: bool,
) -> Result<ArrayRef, String> {
    let name = if date_output {
        "date_slice"
    } else {
        "time_slice"
    };
    if !matches!(args.len(), 3 | 4) {
        return Err(format!(
            "{name} expects value, count, unit and optional boundary"
        ));
    }
    let output_type = if date_output {
        DataType::Date32
    } else {
        DataType::Timestamp(TimeUnit::Microsecond, None)
    };
    if arena.data_type(expr) != Some(&output_type) {
        return Err(format!(
            "{name} result type differs from its frozen temporal domain"
        ));
    }
    // SQL materializes temporal coercions before binding.
    let dt_arr = arena.eval(args[0], chunk)?;
    let interval_arr = arena.eval(args[1], chunk)?;
    let unit_arr = arena.eval(args[2], chunk)?;
    let boundary_arr = if args.len() == 4 {
        Some(arena.eval(args[3], chunk)?)
    } else {
        None
    };
    if dt_arr.data_type() != &output_type || interval_arr.data_type() != &DataType::Int32 {
        return Err(format!(
            "{name} arguments differ from their frozen temporal/INT32 domains"
        ));
    }
    let dts = extract_datetime_array(&dt_arr)?;
    let interval_values = extract_i64_array(&interval_arr, "time_slice")
        .map_err(|_| "time_slice expects int interval".to_string())?;
    let unit_arr = unit_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "time_slice expects unit string".to_string())?;
    let boundary_arr = boundary_arr
        .as_ref()
        .map(|array| {
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| format!("{name} expects boundary string"))
        })
        .transpose()?;
    for length in [interval_values.len(), unit_arr.len()]
        .into_iter()
        .chain(boundary_arr.map(|array| array.len()))
    {
        if length != 1 && length != dts.len() {
            return Err(format!("{name} argument lengths differ"));
        }
    }
    let mut out = if date_output {
        SliceValues::Date(Vec::with_capacity(dts.len()))
    } else {
        SliceValues::Datetime(Vec::with_capacity(dts.len()))
    };
    let start = NaiveDate::from_ymd_opt(1, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    for i in 0..dts.len() {
        let interval = i64_at(&interval_values, i, dts.len())
            .ok_or_else(|| format!("{name} requires non-null interval"))?;
        let unit_str = str_at(unit_arr, i, dts.len())
            .ok_or_else(|| format!("{name} requires non-null unit"))?;
        if interval <= 0 {
            return Err(format!(
                "{name} requires second parameter must be greater than 0"
            ));
        }
        let unit = TimeSliceUnit::parse(unit_str)?;
        if date_output
            && !matches!(
                unit,
                TimeSliceUnit::Year
                    | TimeSliceUnit::Quarter
                    | TimeSliceUnit::Month
                    | TimeSliceUnit::Week
                    | TimeSliceUnit::Day
            )
        {
            return Err("can't use time_slice for date with time(hour/minute/second)".into());
        }
        let boundary = match boundary_arr {
            Some(array) => str_at(array, i, dts.len())
                .ok_or_else(|| format!("{name} requires non-null boundary"))?,
            None => "floor",
        };
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
        let dt = match dts[i] {
            Some(v) => v,
            None => {
                out.push(None)?;
                continue;
            }
        };
        if dt < start {
            return Err("time used with time_slice can't before 0001-01-01 00:00:00".to_string());
        }
        let duration = unit
            .index(dt, start)
            .ok_or_else(|| format!("{name} input exceeds supported slice arithmetic"))?;
        let mut epoch = duration / interval;
        if use_ceil {
            epoch = epoch
                .checked_add(1)
                .ok_or_else(|| format!("{name} bucket index overflow"))?;
        }
        let Some(sliced) = unit.sliced_datetime(start, epoch, interval) else {
            out.push(None)?;
            continue;
        };
        if !(1..=9999).contains(&sliced.year()) {
            out.push(None)?;
            continue;
        }
        out.push(Some(sliced))?;
    }
    Ok(out.finish())
}

pub fn eval_time_slice(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_time_slice_inner(arena, expr, args, chunk, false)
}

pub fn eval_date_slice(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_time_slice_inner(arena, expr, args, chunk, true)
}
