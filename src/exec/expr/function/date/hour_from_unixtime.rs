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
use super::common::extract_i64_array;
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{ArrayRef, Int64Array};
use chrono::{Local, TimeZone, Timelike};
use std::sync::Arc;

const MIN_FROM_UNIXTIME_SECONDS: i64 = 0;
const MAX_FROM_UNIXTIME_SECONDS: i64 = 253_402_243_199;

pub fn eval_hour_from_unixtime(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let arr = arena.eval(args[0], chunk)?;
    let values = extract_i64_array(&arr, "hour_from_unixtime")?;
    let len = values.len();
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        let Some(secs) = values[i] else {
            out.push(None);
            continue;
        };
        if !(MIN_FROM_UNIXTIME_SECONDS..=MAX_FROM_UNIXTIME_SECONDS).contains(&secs) {
            out.push(None);
            continue;
        }
        let local = Local.timestamp_opt(secs, 0).single();
        out.push(local.map(|dt| dt.hour() as i64));
    }
    Ok(Arc::new(Int64Array::from(out)) as ArrayRef)
}
#[cfg(test)]
mod tests {
    use crate::exec::expr::function::date::test_utils::assert_date_function_logic;

    #[test]
    fn test_hour_from_unixtime_logic() {
        assert_date_function_logic("hour_from_unixtime");
    }
}
