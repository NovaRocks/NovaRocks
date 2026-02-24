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

pub fn eval_from_base64(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let input = arena.eval(args[0], chunk)?;
    let input = input
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "from_base64 expects VARCHAR argument".to_string())?;

    let mut out = Vec::with_capacity(chunk.len());
    for row in 0..chunk.len() {
        if input.is_null(row) {
            out.push(None);
            continue;
        }

        let value = input.value(row);
        if value.is_empty() {
            out.push(None);
            continue;
        }

        out.push(super::common::decode_base64(value.as_bytes()));
    }

    super::common::build_bytes_output_latin1(out, arena.data_type(expr))
}

#[cfg(test)]
mod tests {
    use super::eval_from_base64;
    use crate::exec::expr::ExprArena;
    use crate::exec::expr::function::encryption::test_utils::{
        chunk_len_1, literal_string, typed_null,
    };
    use arrow::array::StringArray;
    use arrow::datatypes::DataType;

    #[test]
    fn test_from_base64_basic() {
        let mut arena = ExprArena::default();
        let expr = typed_null(&mut arena, DataType::Utf8);
        let input = literal_string(&mut arena, "SGVsbG8=");

        let out = eval_from_base64(&arena, expr, &[input], &chunk_len_1()).unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out.value(0), "Hello");
    }
}
