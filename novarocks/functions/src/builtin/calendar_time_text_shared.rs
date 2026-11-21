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
//! Original TIME computation. Source occurrence scheduling belongs to the host.
use super::calendar_extended::DateInput;
use crate::{KernelEvaluationControl, KernelFailure, kernel_input::EvaluationCheckpoints};
use arrow_array::{Array, ArrayRef, Int64Array, StringArray};
use arrow_schema::DataType;
use chrono::{NaiveTime, Timelike};
use std::{sync::Arc, time::Duration};
#[path = "calendar_time_text_failure.rs"]
mod failure;
pub use failure::{DurationCastError, TimeTextDataFailure, TimeTextError};
pub struct LegacyControl;
impl KernelEvaluationControl for LegacyControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        unreachable!("TIME computation does not wait")
    }
}
pub fn parse_time(s: &str) -> Option<NaiveTime> {
    NaiveTime::parse_from_str(s, "%H:%M:%S")
        .or_else(|_| NaiveTime::parse_from_str(s, "%H:%M:%S%.f"))
        .ok()
}
pub fn time_to_seconds(time: NaiveTime) -> i64 {
    (time.hour() as i64) * 3600 + (time.minute() as i64) * 60 + (time.second() as i64)
}
pub fn parse_hms_duration_to_seconds(raw: &str) -> Option<i64> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }

    if raw.starts_with('-') {
        return None;
    }
    let s = raw.strip_prefix('+').unwrap_or(raw);
    let mut parts = s.split(':');
    let hour = parts.next()?.parse::<i64>().ok()?;
    let minute = parts.next()?.parse::<i64>().ok()?;
    let second = parts.next()?.parse::<i64>().ok()?;
    if parts.next().is_some()
        || minute >= 60
        || second >= 60
        || hour < 0
        || minute < 0
        || second < 0
    {
        return None;
    }
    hour.checked_mul(3600)?
        .checked_add(minute * 60)?
        .checked_add(second)
}

pub fn duration_strings(
    array: &StringArray,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Vec<Option<i64>>, TimeTextError> {
    work.flush()?;
    let mut out = Vec::with_capacity(array.len());
    work.flush()?;
    for i in 0..array.len() {
        work.step()?;
        let v = if array.is_null(i) {
            None
        } else {
            let s = array.value(i);
            for _ in s.bytes() {
                work.step()?;
            }
            work.flush()?;
            let v = parse_hms_duration_to_seconds(s);
            work.flush()?;
            v
        };
        out.push(v);
    }
    Ok(out)
}
pub fn clock_strings(
    array: &StringArray,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Vec<Option<i64>>, TimeTextError> {
    work.flush()?;
    let mut out = Vec::with_capacity(array.len());
    work.flush()?;
    for i in 0..array.len() {
        work.step()?;
        let v = if array.is_null(i) {
            None
        } else {
            let s = array.value(i);
            for _ in s.bytes() {
                work.step()?;
            }
            work.flush()?;
            let v = parse_time(s).map(time_to_seconds);
            work.flush()?;
            v
        };
        out.push(v);
    }
    Ok(out)
}
pub fn datetime_seconds(
    array: &ArrayRef,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Vec<Option<i64>>, TimeTextError> {
    work.flush()?;
    let reader =
        DateInput::raw(array.as_ref()).map_err(|e| TimeTextError::from(e.into_kernel()))?;
    work.flush()?;
    let mut out = Vec::with_capacity(array.len());
    work.flush()?;
    for row in 0..array.len() {
        work.step()?;
        out.push(if array.is_null(row) {
            None
        } else {
            reader.read(row, work)?.map(|d| time_to_seconds(d.time()))
        });
    }
    Ok(out)
}
const SEC_TO_TIME_CAP_SECONDS: i64 = 839 * 3600 + 59 * 60 + 59;
fn clamp_sec_to_time_seconds(seconds: i64) -> i64 {
    seconds.clamp(-SEC_TO_TIME_CAP_SECONDS, SEC_TO_TIME_CAP_SECONDS)
}

pub fn sec_to_time_source(
    array: &ArrayRef,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Vec<Option<i64>>, TimeTextError> {
    let source = array
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| TimeTextError::contract("sec_to_time source for time_to_sec must be int"))?;
    work.flush()?;
    let mut out = Vec::with_capacity(source.len());
    work.flush()?;
    for row in 0..source.len() {
        work.step()?;
        out.push(if source.is_null(row) {
            None
        } else {
            Some(clamp_sec_to_time_seconds(source.value(row)))
        });
    }
    Ok(out)
}
#[derive(Clone, Copy)]
pub enum MergeMode {
    Override,
    FillNull,
}
pub fn merge_source(
    out: &mut [Option<i64>],
    source: &[Option<i64>],
    mode: MergeMode,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), TimeTextError> {
    for (idx, value) in out.iter_mut().enumerate() {
        work.step()?;
        if idx < source.len() && (matches!(mode, MergeMode::Override) || value.is_none()) {
            *value = source[idx];
        }
    }
    Ok(())
}
pub fn any_null(
    out: &[Option<i64>],
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<bool, TimeTextError> {
    for value in out {
        work.step()?;
        if value.is_none() {
            return Ok(true);
        }
    }
    Ok(false)
}
pub fn format_argument(
    array: &ArrayRef,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Vec<Option<i64>>, TimeTextError> {
    match array.data_type() {
        DataType::Date32 | DataType::Timestamp(_, _) => datetime_seconds(array, work),
        DataType::Utf8 => clock_strings(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| TimeTextError::contract("time_format expects string"))?,
            work,
        ),
        DataType::Null => {
            work.flush()?;
            let out = vec![None; array.len()];
            work.flush()?;
            Ok(out)
        }
        _ => Err(TimeTextError::contract("time_format expects time")),
    }
}
pub fn format_time_pattern_observed(
    fmt: &str,
    seconds_of_day: i64,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<String, TimeTextError> {
    work.flush()?;
    let mut out = String::new();
    let mut chars = fmt.chars().peekable();
    let clamped_seconds = seconds_of_day.clamp(0, 86_399);
    while let Some(ch) = chars.next() {
        work.step()?;
        if ch != '%' {
            out.push(ch);
            continue;
        }
        let Some(token) = chars.next() else {
            out.push('%');
            break;
        };
        work.step()?;
        match token {
            '%' => out.push('%'),
            'H' | 'i' | 's' | 'S' => out.push_str("00"),
            'h' => out.push_str("12"),
            'f' => {
                work.flush()?;
                out.push_str(&format!("{clamped_seconds:06}"));
                work.flush()?;
            }
            _ => {
                out.push('%');
                out.push(token);
            }
        }
    }
    work.flush()?;
    Ok(out)
}

pub fn format_output(
    seconds: &[Option<i64>],
    fmt: &StringArray,
    rows: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, TimeTextError> {
    if seconds.len() != rows && seconds.len() != 1 {
        return Err(TimeTextError::contract(
            "time_format argument length mismatch",
        ));
    }
    if fmt.len() != rows && fmt.len() != 1 {
        return Err(TimeTextError::contract(
            "time_format format length mismatch",
        ));
    }
    work.flush()?;
    std::alloc::Layout::array::<Option<String>>(rows)
        .map_err(|source| TimeTextError::backing::<Option<String>>(rows, source))?;
    let mut out = Vec::new();
    out.try_reserve_exact(rows).map_err(|source| {
        KernelFailure::from_domain(crate::allocation_failure::AllocationRefusal::new(
            "reserve TIME text output",
            source,
        ))
    })?;
    work.flush()?;
    let mut total = 0_u128;
    let mut largest = 0_u128;
    for i in 0..rows {
        work.step()?;
        let time_v = if seconds.len() == 1 {
            seconds[0]
        } else {
            seconds[i]
        };
        let frow = if fmt.len() == 1 { 0 } else { i };
        if fmt.is_null(frow) {
            out.push(None);
            continue;
        }
        let Some(seconds) = time_v else {
            out.push(None);
            continue;
        };
        let value = format_time_pattern_observed(fmt.value(frow), seconds, work)?;
        total += value.len() as u128;
        largest = largest.max(value.len() as u128);
        if total > i32::MAX as u128 {
            return Err(TimeTextError::Extent {
                required: total,
                largest,
                prefix: i,
            });
        }
        out.push(Some(value));
    }
    work.flush()?;
    let array = Arc::new(StringArray::from(out)) as ArrayRef;
    work.flush()?;
    Ok(array)
}
pub fn seconds_output(
    seconds: Vec<Option<i64>>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, TimeTextError> {
    work.flush()?;
    let array = Arc::new(Int64Array::from(seconds)) as ArrayRef;
    work.flush()?;
    Ok(array)
}

pub fn duration_cast_source(
    array: &ArrayRef,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Vec<Option<i64>>, DurationCastError> {
    if let Some(source) = array.as_any().downcast_ref::<StringArray>() {
        return duration_strings(source, work).map_err(Into::into);
    }
    work.flush()?;
    let source = arrow_cast::cast(array, &DataType::Utf8).map_err(|source| {
        // This concrete cast delegates supported temporal values to Arrow's
        // value formatter. Its CastError means a value cannot be formatted;
        // it is distinct from unsupported source types and capacity failures.
        if matches!(
            array.data_type(),
            DataType::Date32
                | DataType::Date64
                | DataType::Timestamp(_, _)
                | DataType::Time32(_)
                | DataType::Time64(_)
        ) && matches!(source, arrow_schema::ArrowError::CastError(_))
        {
            DurationCastError::Data(TimeTextDataFailure::new(source))
        } else if matches!(
            array.data_type(),
            DataType::LargeUtf8 | DataType::LargeBinary
        ) && matches!(source, arrow_schema::ArrowError::ComputeError(_))
        {
            // Pinned Large->small byte-container cast only produces this
            // ComputeError when an existing offset cannot enter i32.
            DurationCastError::Outer(TimeTextError::OffsetNarrow(source))
        } else {
            DurationCastError::Outer(TimeTextError::Arrow(source))
        }
    })?;
    work.flush()?;
    let source = source
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            TimeTextError::contract("TIME UTF8 cast returned an incompatible carrier")
        })?;
    duration_strings(source, work).map_err(Into::into)
}
