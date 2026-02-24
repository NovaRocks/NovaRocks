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
use arrow::array::{ArrayRef, StringArray};
use std::sync::Arc;

use super::common::{OLAP_STRING_MAX_LENGTH, downcast_int_arg_array};

pub fn eval_space(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let _ = expr;
    let num_arr = arena.eval(args[0], chunk)?;
    let arr = downcast_int_arg_array(&num_arr, "space")?;
    let len = arr.len();
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        if arr.is_null(i) {
            out.push(None);
            continue;
        }
        let n = arr.value(i);
        if n < 0 {
            out.push(None);
            continue;
        }
        if (n as u128) > OLAP_STRING_MAX_LENGTH as u128 {
            out.push(None);
            continue;
        }
        out.push(Some(" ".repeat(n as usize)));
    }
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}
#[cfg(test)]
mod tests {
    use crate::exec::expr::function::string::test_utils::assert_string_function_logic;

    #[test]
    fn test_space_logic() {
        assert_string_function_logic("space");
    }
}
