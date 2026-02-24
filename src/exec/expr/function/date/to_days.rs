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
use super::common::{BC_EPOCH_JULIAN, extract_date_array, julian_from_date};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{ArrayRef, Int64Array};
use std::sync::Arc;

pub fn eval_to_days(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let arr = arena.eval(args[0], chunk)?;
    let dates = extract_date_array(&arr)?;
    let mut out = Vec::with_capacity(dates.len());
    for date in dates {
        let v = date.map(|d| (julian_from_date(d) - BC_EPOCH_JULIAN) as i64);
        out.push(v);
    }
    Ok(Arc::new(Int64Array::from(out)) as ArrayRef)
}
#[cfg(test)]
mod tests {
    use crate::exec::expr::function::date::test_utils::assert_date_function_logic;

    #[test]
    fn test_to_days_logic() {
        assert_date_function_logic("to_days");
    }
}
