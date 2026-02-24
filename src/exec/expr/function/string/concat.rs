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

use super::common::OLAP_STRING_MAX_LENGTH;

pub fn eval_concat(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let _ = expr;
    let mut arrays = Vec::with_capacity(args.len());
    for arg in args {
        arrays.push(
            arena
                .eval(*arg, chunk)?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "concat expects string".to_string())?
                .clone(),
        );
    }
    let len = chunk.len();
    let mut out = Vec::with_capacity(len);
    for row in 0..len {
        let mut total_len = 0usize;
        let mut is_null_or_oversize = false;
        for arr in &arrays {
            if arr.is_null(row) {
                is_null_or_oversize = true;
                break;
            }
            let value = arr.value(row);
            total_len = total_len.saturating_add(value.len());
            if total_len > OLAP_STRING_MAX_LENGTH {
                is_null_or_oversize = true;
                break;
            }
        }
        if is_null_or_oversize {
            out.push(None);
        } else {
            let mut buf = String::with_capacity(total_len);
            for arr in &arrays {
                buf.push_str(arr.value(row));
            }
            out.push(Some(buf));
        }
    }
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}
#[cfg(test)]
mod tests {
    use crate::exec::expr::function::string::test_utils::assert_string_function_logic;

    #[test]
    fn test_concat_logic() {
        assert_string_function_logic("concat");
    }
}
