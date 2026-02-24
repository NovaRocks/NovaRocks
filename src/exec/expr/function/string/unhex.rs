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
use arrow::array::{Array, ArrayRef, BinaryBuilder, StringArray};
use std::sync::Arc;

pub fn eval_unhex(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let _ = expr;
    let str_arr = arena.eval(args[0], chunk)?;
    let s_arr = str_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "unhex expects string".to_string())?;
    let mut builder = BinaryBuilder::new();
    for i in 0..s_arr.len() {
        if s_arr.is_null(i) {
            builder.append_null();
            continue;
        }
        let s = s_arr.value(i);
        let value = hex::decode(s).unwrap_or_default();
        builder.append_value(value);
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}
#[cfg(test)]
mod tests {
    use super::eval_unhex;
    use crate::exec::expr::ExprArena;
    use crate::exec::expr::function::string::test_utils::assert_string_function_logic;
    use crate::exec::expr::function::string::test_utils::{
        chunk_len_1, literal_string, typed_null,
    };
    use arrow::array::{Array, BinaryArray};
    use arrow::datatypes::DataType;

    #[test]
    fn test_unhex_logic() {
        assert_string_function_logic("unhex");
    }

    #[test]
    fn test_unhex_invalid_input_returns_empty_string() {
        let mut arena = ExprArena::default();
        let expr = typed_null(&mut arena, DataType::Binary);
        let bad = literal_string(&mut arena, "ZZ");
        let odd = literal_string(&mut arena, "F");

        let out_bad = eval_unhex(&arena, expr, &[bad], &chunk_len_1()).unwrap();
        let out_bad = out_bad.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert!(!out_bad.is_null(0));
        assert_eq!(out_bad.value(0), b"");

        let out_odd = eval_unhex(&arena, expr, &[odd], &chunk_len_1()).unwrap();
        let out_odd = out_odd.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert!(!out_odd.is_null(0));
        assert_eq!(out_odd.value(0), b"");
    }
}
