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

pub fn eval_format_bytes(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let _ = expr;
    let num_arr = arena.eval(args[0], chunk)?;
    let arr = num_arr
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .ok_or_else(|| "format_bytes expects integer".to_string())?;
    let len = arr.len();
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        if arr.is_null(i) {
            out.push(None);
            continue;
        }
        let bytes = arr.value(i);
        if bytes < 0 {
            out.push(None);
            continue;
        }

        let units = ["B", "KB", "MB", "GB", "TB", "PB", "EB"];
        let mut size = bytes as f64;
        let mut unit = 0usize;
        while size >= 1024.0 && unit < units.len() - 1 {
            size /= 1024.0;
            unit += 1;
        }
        let formatted = if unit == 0 {
            format!("{bytes} B")
        } else {
            format!("{:.2} {}", size, units[unit])
        };
        out.push(Some(formatted));
    }
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}
#[cfg(test)]
mod tests {
    use crate::exec::expr::function::string::test_utils::assert_string_function_logic;

    #[test]
    fn test_format_bytes_logic() {
        assert_string_function_logic("format_bytes");
    }
}
