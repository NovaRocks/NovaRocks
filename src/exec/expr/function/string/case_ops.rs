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

fn eval_lower_impl(arena: &ExprArena, args: &[ExprId], chunk: &Chunk) -> Result<ArrayRef, String> {
    let input = arena.eval(args[0], chunk)?;
    let arr = input
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "lower expects string".to_string())?;
    let len = arr.len();
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        if arr.is_null(i) {
            out.push(None);
        } else {
            out.push(Some(arr.value(i).to_lowercase()));
        }
    }
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}

pub fn eval_lower(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_lower_impl(arena, args, chunk)
}

pub fn eval_lcase(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_lower_impl(arena, args, chunk)
}

#[cfg(test)]
mod tests {
    use crate::exec::expr::function::string::test_utils::assert_string_function_logic;

    #[test]
    fn test_lower_logic() {
        assert_string_function_logic("lower");
    }

    #[test]
    fn test_lcase_logic() {
        assert_string_function_logic("lcase");
    }
}
