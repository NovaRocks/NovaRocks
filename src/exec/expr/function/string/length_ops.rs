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
use arrow::array::{Array, ArrayRef, Int32Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use std::sync::Arc;

fn eval_char_length_impl(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    utf8: bool,
) -> Result<ArrayRef, String> {
    let str_arr = arena.eval(args[0], chunk)?;
    let s_arr = str_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "length expects string".to_string())?;
    let len = s_arr.len();
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        if s_arr.is_null(i) {
            out.push(None);
            continue;
        }
        let s = s_arr.value(i);
        let v = if utf8 {
            s.chars().count()
        } else {
            s.as_bytes().len()
        } as i64;
        out.push(Some(v));
    }
    match arena
        .data_type(expr)
        .ok_or_else(|| "length return type is missing".to_string())?
    {
        DataType::Int32 => {
            let mut out_i32 = Vec::with_capacity(out.len());
            for value in out {
                let v = match value {
                    Some(v) => Some(
                        i32::try_from(v)
                            .map_err(|_| format!("length result out of INT range: {v}"))?,
                    ),
                    None => None,
                };
                out_i32.push(v);
            }
            Ok(Arc::new(Int32Array::from(out_i32)) as ArrayRef)
        }
        DataType::Int64 => Ok(Arc::new(Int64Array::from(out)) as ArrayRef),
        other => Err(format!(
            "length return type must be INT/BIGINT, got {:?}",
            other
        )),
    }
}

pub fn eval_length(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_char_length_impl(arena, expr, args, chunk, false)
}

pub fn eval_char_length(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_char_length_impl(arena, expr, args, chunk, true)
}

#[cfg(test)]
mod tests {
    use arrow::array::Int32Array;
    use arrow::datatypes::DataType;

    use crate::exec::expr::ExprArena;
    use crate::exec::expr::function::string::test_utils::assert_string_function_logic;
    use crate::exec::expr::function::string::test_utils::{
        chunk_len_1, literal_string, typed_null,
    };

    use super::eval_length;

    #[test]
    fn test_length_logic() {
        assert_string_function_logic("length");
    }

    #[test]
    fn test_char_length_logic() {
        assert_string_function_logic("char_length");
    }

    #[test]
    fn test_length_respects_int_return_type() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_i32 = typed_null(&mut arena, DataType::Int32);
        let s = literal_string(&mut arena, "abc");
        let out = eval_length(&arena, expr_i32, &[s], &chunk).expect("eval length");
        let arr = out
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("length should produce Int32Array");
        assert_eq!(arr.value(0), 3);
    }
}
