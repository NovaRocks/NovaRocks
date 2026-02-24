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
use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use std::sync::Arc;

const SEC_TO_TIME_CAP_SECONDS: i64 = 839 * 3600 + 59 * 60 + 59;

fn format_sec_to_time(seconds: i64) -> String {
    let clamped = seconds.clamp(-SEC_TO_TIME_CAP_SECONDS, SEC_TO_TIME_CAP_SECONDS);
    let sign = if clamped < 0 { "-" } else { "" };
    let abs = clamped.abs();
    let hour = abs / 3600;
    let minute = (abs % 3600) / 60;
    let second = abs % 60;
    format!("{sign}{hour:02}:{minute:02}:{second:02}")
}

pub fn eval_sec_to_time(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let arr = arena.eval(args[0], chunk)?;
    let arr = arr
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "sec_to_time expects int".to_string())?;
    let mut out = Vec::with_capacity(arr.len());
    for i in 0..arr.len() {
        if arr.is_null(i) {
            out.push(None);
        } else {
            out.push(Some(format_sec_to_time(arr.value(i))));
        }
    }
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}
#[cfg(test)]
mod tests {
    use super::format_sec_to_time;
    use crate::exec::expr::function::date::test_utils::assert_date_function_logic;

    #[test]
    fn test_sec_to_time_logic() {
        assert_date_function_logic("sec_to_time");
    }

    #[test]
    fn test_sec_to_time_boundaries() {
        assert_eq!(format_sec_to_time(-1), "-00:00:01");
        assert_eq!(format_sec_to_time(0), "00:00:00");
        assert_eq!(format_sec_to_time(90061), "25:01:01");
        assert_eq!(format_sec_to_time(3020399), "838:59:59");
        assert_eq!(format_sec_to_time(3020400), "839:00:00");
        assert_eq!(format_sec_to_time(3023999), "839:59:59");
        assert_eq!(format_sec_to_time(3024000), "839:59:59");
    }
}
