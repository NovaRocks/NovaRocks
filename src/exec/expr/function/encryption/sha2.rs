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
use sha2::{Digest, Sha224, Sha256, Sha384, Sha512};
use std::sync::Arc;

pub fn eval_sha2(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let _ = expr;
    let input = super::common::to_owned_bytes_array(arena.eval(args[0], chunk)?, "sha2", 0)?;
    let length = super::common::to_i64_array(&arena.eval(args[1], chunk)?, "sha2", 1)?;

    let mut out = Vec::with_capacity(chunk.len());
    for row in 0..chunk.len() {
        if input.is_null(row) || length.is_null(row) {
            out.push(None);
            continue;
        }

        let hash_len = length.value(row);
        let bytes = input.bytes(row);
        let digest = match hash_len {
            224 => {
                let mut h = Sha224::new();
                h.update(bytes);
                Some(hex::encode(h.finalize()))
            }
            0 | 256 => {
                let mut h = Sha256::new();
                h.update(bytes);
                Some(hex::encode(h.finalize()))
            }
            384 => {
                let mut h = Sha384::new();
                h.update(bytes);
                Some(hex::encode(h.finalize()))
            }
            512 => {
                let mut h = Sha512::new();
                h.update(bytes);
                Some(hex::encode(h.finalize()))
            }
            _ => None,
        };
        out.push(digest);
    }

    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::eval_sha2;
    use crate::exec::expr::ExprArena;
    use crate::exec::expr::function::encryption::test_utils::{
        chunk_len_1, literal_string, typed_null,
    };
    use crate::exec::expr::{ExprId, ExprNode, LiteralValue};
    use arrow::array::StringArray;
    use arrow::datatypes::DataType;

    fn literal_i64(arena: &mut ExprArena, v: i64) -> ExprId {
        arena.push(ExprNode::Literal(LiteralValue::Int64(v)))
    }

    #[test]
    fn test_sha2_256_basic() {
        let mut arena = ExprArena::default();
        let expr = typed_null(&mut arena, DataType::Utf8);
        let s = literal_string(&mut arena, "abc");
        let len = literal_i64(&mut arena, 256);

        let out = eval_sha2(&arena, expr, &[s, len], &chunk_len_1()).unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            out.value(0),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
