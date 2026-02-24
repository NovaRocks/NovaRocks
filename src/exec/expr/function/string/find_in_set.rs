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
use arrow::array::{Array, ArrayRef, StringArray};
use std::sync::Arc;

pub fn eval_find_in_set(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let _ = expr;
    let str_arr = arena.eval(args[0], chunk)?;
    let set_arr = arena.eval(args[1], chunk)?;
    let s_arr = str_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "find_in_set expects string".to_string())?;
    let set_arr = set_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "find_in_set expects string".to_string())?;
    let len = s_arr.len();
    let mut out = Vec::with_capacity(len);
    for row in 0..len {
        if s_arr.is_null(row) || set_arr.is_null(row) {
            out.push(None);
            continue;
        }
        let target = s_arr.value(row);
        if target.contains(',') {
            out.push(Some(0_i64));
            continue;
        }
        let set = set_arr.value(row);
        let mut idx = 0_i64;
        for (i, part) in set.split(',').enumerate() {
            if part == target {
                idx = (i + 1) as i64;
                break;
            }
        }
        out.push(Some(idx));
    }
    Ok(Arc::new(arrow::array::Int64Array::from(out)) as ArrayRef)
}
#[cfg(test)]
mod tests {
    use crate::exec::expr::function::string::test_utils::assert_string_function_logic;

    #[test]
    fn test_find_in_set_logic() {
        assert_string_function_logic("find_in_set");
    }
}
