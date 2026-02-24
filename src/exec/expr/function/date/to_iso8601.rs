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
use super::common::{extract_date_array, extract_datetime_array};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::DataType;
use std::sync::Arc;

pub fn eval_to_iso8601(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let arr = arena.eval(args[0], chunk)?;
    let out = match arr.data_type() {
        DataType::Date32 => extract_date_array(&arr)?
            .into_iter()
            .map(|d| d.map(|v| v.format("%Y-%m-%d").to_string()))
            .collect::<Vec<_>>(),
        _ => extract_datetime_array(&arr)?
            .into_iter()
            .map(|dt| {
                dt.map(|v| {
                    format!(
                        "{}.{:06}",
                        v.format("%Y-%m-%dT%H:%M:%S"),
                        v.and_utc().timestamp_subsec_micros()
                    )
                })
            })
            .collect::<Vec<_>>(),
    };
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use crate::exec::expr::function::date::test_utils::assert_date_function_logic;

    #[test]
    fn test_to_iso8601_logic() {
        assert_date_function_logic("to_iso8601");
    }
}
