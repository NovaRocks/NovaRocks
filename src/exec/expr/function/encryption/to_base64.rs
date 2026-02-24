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
use crate::exec::expr::function::FunctionKind;
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use arrow::array::{ArrayRef, StringArray};
use std::sync::Arc;

fn should_prefer_latin1_bytes(arena: &ExprArena, arg: ExprId) -> bool {
    matches!(
        arena.node(arg),
        Some(ExprNode::FunctionCall {
            kind: FunctionKind::Encryption(name),
            ..
        }) if matches!(*name, "aes_encrypt" | "from_base64" | "to_binary")
    )
}

pub fn eval_to_base64(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let _ = expr;
    let prefer_latin1 = should_prefer_latin1_bytes(arena, args[0]);
    let input = super::common::to_owned_bytes_array(arena.eval(args[0], chunk)?, "to_base64", 0)?;

    let mut out = Vec::with_capacity(chunk.len());
    for row in 0..chunk.len() {
        if input.is_null(row) {
            out.push(None);
            continue;
        }

        let fallback;
        let bytes = if prefer_latin1 {
            if let Some(s) = input.utf8(row) {
                fallback = super::common::latin1_string_to_bytes(s);
                fallback.as_deref().unwrap_or_else(|| input.bytes(row))
            } else {
                input.bytes(row)
            }
        } else {
            input.bytes(row)
        };
        if bytes.is_empty() {
            out.push(None);
            continue;
        }

        out.push(Some(super::common::encode_base64(bytes)));
    }

    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::eval_to_base64;
    use crate::exec::expr::ExprArena;
    use crate::exec::expr::function::encryption::test_utils::{
        chunk_len_1, literal_string, typed_null,
    };
    use arrow::array::StringArray;
    use arrow::datatypes::DataType;

    #[test]
    fn test_to_base64_basic() {
        let mut arena = ExprArena::default();
        let expr = typed_null(&mut arena, DataType::Utf8);
        let input = literal_string(&mut arena, "Hello");

        let out = eval_to_base64(&arena, expr, &[input], &chunk_len_1()).unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out.value(0), "SGVsbG8=");
    }
}
