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

use super::common::{OLAP_STRING_MAX_LENGTH, downcast_int_arg_array};

pub fn eval_repeat(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let _ = expr;
    let str_arr = arena.eval(args[0], chunk)?;
    let num_arr = arena.eval(args[1], chunk)?;
    let s_arr = str_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "repeat expects string".to_string())?;
    let n_arr = downcast_int_arg_array(&num_arr, "repeat")?;
    let len = s_arr.len();
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        if s_arr.is_null(i) || n_arr.is_null(i) {
            out.push(None);
            continue;
        }
        let n = n_arr.value(i);
        if n < 0 {
            out.push(Some(String::new()));
            continue;
        }
        let s = s_arr.value(i);
        if s.is_empty() || n == 0 {
            out.push(Some(String::new()));
            continue;
        }
        let n_u128 = n as u128;
        let bytes_u128 = s.len() as u128;
        if bytes_u128.saturating_mul(n_u128) > OLAP_STRING_MAX_LENGTH as u128 {
            out.push(None);
            continue;
        }
        out.push(Some(s.repeat(n as usize)));
    }
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}
#[cfg(test)]
mod tests {
    use crate::exec::expr::function::string::test_utils::assert_string_function_logic;

    #[test]
    fn test_repeat_logic() {
        assert_string_function_logic("repeat");
    }
}
