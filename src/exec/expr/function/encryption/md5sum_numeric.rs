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
use crate::common::largeint;
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{ArrayRef, Int64Array};
use md5::{Digest, Md5};
use std::sync::Arc;

pub fn eval_md5sum_numeric(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let mut inputs = Vec::with_capacity(args.len());
    for (idx, arg) in args.iter().enumerate() {
        inputs.push(super::common::to_owned_bytes_array(
            arena.eval(*arg, chunk)?,
            "md5sum_numeric",
            idx,
        )?);
    }

    let output_type = arena.data_type(expr);
    if output_type
        .map(largeint::is_largeint_data_type)
        .unwrap_or(false)
    {
        let mut out = Vec::with_capacity(chunk.len());
        for row in 0..chunk.len() {
            let mut hasher = Md5::new();
            for input in &inputs {
                if input.is_null(row) {
                    continue;
                }
                hasher.update(input.bytes(row));
            }

            let digest = hasher.finalize();
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(&digest[..16]);
            out.push(Some(i128::from_be_bytes(bytes)));
        }
        return largeint::array_from_i128(&out);
    }

    let mut out_i64 = Vec::with_capacity(chunk.len());
    for row in 0..chunk.len() {
        let mut hasher = Md5::new();
        for input in &inputs {
            if input.is_null(row) {
                continue;
            }
            hasher.update(input.bytes(row));
        }

        let digest = hasher.finalize();
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&digest[..16]);
        let value = u128::from_be_bytes(bytes) as i64;
        out_i64.push(Some(value));
    }

    let out = Arc::new(Int64Array::from(out_i64)) as ArrayRef;
    super::common::cast_output(out, output_type, "md5sum_numeric")
}

#[cfg(test)]
mod tests {
    use super::eval_md5sum_numeric;
    use crate::common::largeint;
    use crate::exec::expr::ExprArena;
    use crate::exec::expr::function::encryption::test_utils::{
        chunk_len_1, literal_string, typed_null,
    };
    use arrow::array::{FixedSizeBinaryArray, Int64Array};
    use arrow::datatypes::DataType;

    #[test]
    fn test_md5sum_numeric_basic() {
        let mut arena = ExprArena::default();
        let expr = typed_null(&mut arena, DataType::Int64);
        let a = literal_string(&mut arena, "ab");

        let out = eval_md5sum_numeric(&arena, expr, &[a], &chunk_len_1()).unwrap();
        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();

        let expected_u128 = u128::from_str_radix("187ef4436122d1cc2f40dc2b92f0eba0", 16).unwrap();
        assert_eq!(out.value(0), expected_u128 as i64);
    }

    #[test]
    fn test_md5sum_numeric_largeint_output() {
        let mut arena = ExprArena::default();
        let expr = typed_null(&mut arena, DataType::FixedSizeBinary(16));
        let a = literal_string(&mut arena, "abc");

        let out = eval_md5sum_numeric(&arena, expr, &[a], &chunk_len_1()).unwrap();
        let out = out.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
        let parsed = largeint::i128_from_be_bytes(out.value(0)).unwrap();
        assert_eq!(parsed, -148866708576779697295343134153845407886_i128);
    }
}
