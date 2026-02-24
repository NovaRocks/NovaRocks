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
use sm3::{Digest, Sm3};
use std::sync::Arc;

fn format_sm3_with_spaces(bytes: &[u8]) -> String {
    let mut out = String::new();
    for (idx, byte) in bytes.iter().enumerate() {
        if idx >= 4 && idx % 4 == 0 {
            out.push(' ');
        }
        out.push_str(&format!("{:02x}", byte));
    }
    out
}

pub fn eval_sm3(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let _ = expr;
    let input = super::common::to_owned_bytes_array(arena.eval(args[0], chunk)?, "sm3", 0)?;

    let mut out = Vec::with_capacity(chunk.len());
    for row in 0..chunk.len() {
        if input.is_null(row) {
            out.push(None);
            continue;
        }

        let bytes = input.bytes(row);
        if bytes.is_empty() {
            out.push(Some(String::new()));
            continue;
        }

        let mut hasher = Sm3::new();
        hasher.update(bytes);
        let digest = hasher.finalize();
        out.push(Some(format_sm3_with_spaces(&digest)));
    }

    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::eval_sm3;
    use crate::exec::expr::ExprArena;
    use crate::exec::expr::function::encryption::test_utils::{
        chunk_len_1, literal_string, typed_null,
    };
    use arrow::array::StringArray;
    use arrow::datatypes::DataType;

    #[test]
    fn test_sm3_basic() {
        let mut arena = ExprArena::default();
        let expr = typed_null(&mut arena, DataType::Utf8);
        let input = literal_string(&mut arena, "abc");

        let out = eval_sm3(&arena, expr, &[input], &chunk_len_1()).unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            out.value(0),
            "66c7f0f4 62eeedd9 d1f2d46b dc10e4e2 4167c487 5cf2f7a2 297da02b 8f4ba8e0"
        );
    }
}
