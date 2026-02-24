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
use super::common::{extract_datetime_array, parse_time, seconds_to_time, to_timestamp_value};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{ArrayRef, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::DataType;
use chrono::NaiveDate;
use std::sync::Arc;

pub fn eval_timediff(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let dt1 = extract_datetime_array(&arena.eval(args[0], chunk)?)?;
    let dt2 = extract_datetime_array(&arena.eval(args[1], chunk)?)?;
    let output_type = arena.data_type(expr).cloned().unwrap_or(DataType::Utf8);
    let mut out = Vec::with_capacity(dt1.len());
    for i in 0..dt1.len() {
        let v = match (dt1[i], dt2[i]) {
            (Some(a), Some(b)) => {
                let secs = (a - b).num_seconds();
                let t = seconds_to_time(secs);
                Some(t.format("%H:%M:%S").to_string())
            }
            _ => None,
        };
        out.push(v);
    }
    if matches!(output_type, DataType::Utf8) {
        Ok(Arc::new(StringArray::from(out)) as ArrayRef)
    } else {
        let mut ts_out = Vec::with_capacity(out.len());
        let base = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        for v in out {
            let dt = v.and_then(|s| parse_time(&s).map(|t| base.and_time(t)));
            let ts = dt.and_then(|d| to_timestamp_value(d, &output_type).ok());
            ts_out.push(ts);
        }
        Ok(Arc::new(TimestampMicrosecondArray::from(ts_out)) as ArrayRef)
    }
}
#[cfg(test)]
mod tests {
    use crate::exec::expr::function::date::test_utils::assert_date_function_logic;

    #[test]
    fn test_timediff_logic() {
        assert_date_function_logic("timediff");
    }
}
