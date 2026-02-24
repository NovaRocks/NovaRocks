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
use super::common::extract_datetime_array;
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use chrono::Datelike;
use std::sync::Arc;

pub fn eval_timestampdiff(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let unit_arr = arena.eval(args[0], chunk)?;
    let dt1_arr = arena.eval(args[1], chunk)?;
    let dt2_arr = arena.eval(args[2], chunk)?;
    let unit_arr = unit_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "timestampdiff expects unit string".to_string())?;
    let d1 = extract_datetime_array(&dt1_arr)?;
    let d2 = extract_datetime_array(&dt2_arr)?;
    let mut out = Vec::with_capacity(d1.len());
    for i in 0..d1.len() {
        if unit_arr.is_null(i) {
            out.push(None);
            continue;
        }
        let unit = unit_arr.value(i).to_lowercase();
        let v = match (d1[i], d2[i]) {
            (Some(a), Some(b)) => {
                let diff = b - a;
                let val = match unit.as_str() {
                    "year" => (b.year() - a.year()) as i64,
                    "month" => {
                        ((b.year() - a.year()) * 12 + (b.month() as i32 - a.month() as i32)) as i64
                    }
                    "week" => diff.num_weeks(),
                    "day" => diff.num_days(),
                    "hour" => diff.num_hours(),
                    "minute" => diff.num_minutes(),
                    "second" => diff.num_seconds(),
                    "millisecond" => diff.num_milliseconds(),
                    _ => {
                        return Err(
                            "unit of timestampdiff must be one of year/month/week/day/hour/minute/second/millisecond"
                                .to_string(),
                        );
                    }
                };
                Some(val)
            }
            _ => None,
        };
        out.push(v);
    }
    Ok(Arc::new(Int64Array::from(out)) as ArrayRef)
}
#[cfg(test)]
mod tests {
    use crate::exec::expr::function::date::test_utils::assert_date_function_logic;

    #[test]
    fn test_timestampdiff_logic() {
        assert_date_function_logic("timestampdiff");
    }
}
