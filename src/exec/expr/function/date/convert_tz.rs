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
use super::common::{extract_datetime_array, to_timestamp_value};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{Array, ArrayRef, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{FixedOffset, TimeZone, Utc};
use chrono_tz::Tz;
use std::str::FromStr;
use std::sync::Arc;

pub fn eval_convert_tz(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let dt_arr = arena.eval(args[0], chunk)?;
    let from_arr = arena.eval(args[1], chunk)?;
    let to_arr = arena.eval(args[2], chunk)?;
    let dts = extract_datetime_array(&dt_arr)?;
    let from_arr = from_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "convert_tz expects string".to_string())?;
    let to_arr = to_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "convert_tz expects string".to_string())?;
    let output_type = arena
        .data_type(expr)
        .cloned()
        .unwrap_or(DataType::Timestamp(TimeUnit::Microsecond, None));
    let mut out = Vec::with_capacity(dts.len());
    for i in 0..dts.len() {
        if from_arr.is_null(i) || to_arr.is_null(i) {
            out.push(None);
            continue;
        }
        let from = parse_tz(from_arr.value(i));
        let to = parse_tz(to_arr.value(i));
        let v = match (dts[i], from, to) {
            (Some(dt), Some(f), Some(t)) => convert_tz_with_zone(dt, f, t)
                .and_then(|ndt| to_timestamp_value(ndt, &output_type).ok()),
            _ => None,
        };
        out.push(v);
    }
    Ok(Arc::new(TimestampMicrosecondArray::from(out)) as ArrayRef)
}

#[derive(Clone, Copy)]
enum TimeZoneSpec {
    Fixed(FixedOffset),
    Named(Tz),
}

fn parse_tz(s: &str) -> Option<TimeZoneSpec> {
    if s.eq_ignore_ascii_case("utc") {
        return FixedOffset::east_opt(0).map(TimeZoneSpec::Fixed);
    }
    if let Ok(offset) = FixedOffset::from_str(s) {
        return Some(TimeZoneSpec::Fixed(offset));
    }
    Tz::from_str(s).ok().map(TimeZoneSpec::Named)
}

fn to_utc_datetime(dt: chrono::NaiveDateTime, from: TimeZoneSpec) -> Option<chrono::DateTime<Utc>> {
    match from {
        TimeZoneSpec::Fixed(offset) => offset
            .from_local_datetime(&dt)
            .single()
            .map(|v| v.with_timezone(&Utc)),
        TimeZoneSpec::Named(tz) => tz
            .from_local_datetime(&dt)
            .single()
            .or_else(|| tz.from_local_datetime(&dt).earliest())
            .or_else(|| tz.from_local_datetime(&dt).latest())
            .map(|v| v.with_timezone(&Utc)),
    }
}

fn convert_tz_with_zone(
    dt: chrono::NaiveDateTime,
    from: TimeZoneSpec,
    to: TimeZoneSpec,
) -> Option<chrono::NaiveDateTime> {
    let utc = to_utc_datetime(dt, from)?;
    let out = match to {
        TimeZoneSpec::Fixed(offset) => utc.with_timezone(&offset).naive_local(),
        TimeZoneSpec::Named(tz) => utc.with_timezone(&tz).naive_local(),
    };
    Some(out)
}
#[cfg(test)]
mod tests {
    use crate::exec::expr::function::date::test_utils::assert_date_function_logic;

    #[test]
    fn test_convert_tz_logic() {
        assert_date_function_logic("convert_tz");
    }
}
