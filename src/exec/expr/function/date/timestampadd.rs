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
use arrow::array::{Array, ArrayRef, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::Duration;
use std::sync::Arc;

pub fn eval_timestampadd(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let unit_arr = arena.eval(args[0], chunk)?;
    let interval_arr = arena.eval(args[1], chunk)?;
    let dt_arr = arena.eval(args[2], chunk)?;
    let unit_arr = unit_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "timestampadd expects unit string".to_string())?;
    let interval_arr = interval_arr
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "timestampadd expects int interval".to_string())?;
    let dts = extract_datetime_array(&dt_arr)?;
    let output_type = arena
        .data_type(expr)
        .cloned()
        .unwrap_or(DataType::Timestamp(TimeUnit::Microsecond, None));
    let mut out = Vec::with_capacity(dts.len());
    for i in 0..dts.len() {
        if unit_arr.is_null(i) || interval_arr.is_null(i) {
            out.push(None);
            continue;
        }
        let dt = match dts[i] {
            Some(v) => v,
            None => {
                out.push(None);
                continue;
            }
        };
        let interval = interval_arr.value(i);
        let unit = unit_arr.value(i).to_lowercase();
        let new_dt = match unit.as_str() {
            "year" => super::add_months::add_months_to_datetime(dt, interval as i32 * 12),
            "month" => super::add_months::add_months_to_datetime(dt, interval as i32),
            "day" => dt + Duration::days(interval),
            "hour" => dt + Duration::hours(interval),
            "minute" => dt + Duration::minutes(interval),
            "second" => dt + Duration::seconds(interval),
            _ => dt,
        };
        out.push(to_timestamp_value(new_dt, &output_type).ok());
    }
    Ok(Arc::new(TimestampMicrosecondArray::from(out)) as ArrayRef)
}
#[cfg(test)]
mod tests {
    use crate::exec::expr::function::date::test_utils::assert_date_function_logic;

    #[test]
    fn test_timestampadd_logic() {
        assert_date_function_logic("timestampadd");
    }
}
