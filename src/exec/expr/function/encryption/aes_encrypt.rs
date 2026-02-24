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
use arrow::array::ArrayRef;

pub fn eval_aes_encrypt(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() != 2 && args.len() != 4 && args.len() != 5 {
        return Err("aes_encrypt expects 2, 4, or 5 arguments".to_string());
    }

    let src = super::common::to_owned_bytes_array(arena.eval(args[0], chunk)?, "aes_encrypt", 0)?;
    let key = super::common::to_owned_bytes_array(arena.eval(args[1], chunk)?, "aes_encrypt", 1)?;

    let iv = if args.len() >= 4 {
        Some(super::common::to_owned_bytes_array(
            arena.eval(args[2], chunk)?,
            "aes_encrypt",
            2,
        )?)
    } else {
        None
    };

    let mode = if args.len() >= 4 {
        Some(super::common::to_owned_bytes_array(
            arena.eval(args[3], chunk)?,
            "aes_encrypt",
            3,
        )?)
    } else {
        None
    };

    let aad = if args.len() == 5 {
        Some(super::common::to_owned_bytes_array(
            arena.eval(args[4], chunk)?,
            "aes_encrypt",
            4,
        )?)
    } else {
        None
    };

    let mut out = Vec::with_capacity(chunk.len());
    for row in 0..chunk.len() {
        if src.is_null(row) || key.is_null(row) {
            out.push(None);
            continue;
        }

        if args.len() == 2 {
            out.push(super::common::aes_encrypt_raw(
                super::common::AesMode::Aes128Ecb,
                src.bytes(row),
                key.bytes(row),
                None,
                None,
            ));
            continue;
        }

        let mode_arr = mode.as_ref().unwrap();
        if mode_arr.is_null(row) {
            out.push(None);
            continue;
        }

        let mode = super::common::AesMode::parse(mode_arr.bytes(row));
        let iv_arr = iv.as_ref().unwrap();

        if !mode.is_ecb() && iv_arr.is_null(row) {
            out.push(None);
            continue;
        }

        let iv_bytes = if iv_arr.is_null(row) {
            None
        } else {
            Some(iv_arr.bytes(row))
        };
        let aad_bytes = aad
            .as_ref()
            .and_then(|arr| (!arr.is_null(row)).then_some(arr.bytes(row)));

        out.push(super::common::aes_encrypt_raw(
            mode,
            src.bytes(row),
            key.bytes(row),
            iv_bytes,
            aad_bytes,
        ));
    }

    super::common::build_bytes_output_latin1(out, arena.data_type(expr))
}

#[cfg(test)]
mod tests {
    use super::eval_aes_encrypt;
    use crate::exec::expr::ExprArena;
    use crate::exec::expr::function::encryption::test_utils::{
        chunk_len_1, literal_string, typed_null,
    };
    use arrow::array::StringArray;
    use arrow::datatypes::DataType;

    #[test]
    fn test_aes_encrypt_basic_roundtrip_raw() {
        let mut arena = ExprArena::default();
        let expr = typed_null(&mut arena, DataType::Utf8);
        let data = literal_string(&mut arena, "hello world");
        let key = literal_string(&mut arena, "starrocks-key");

        let out = eval_aes_encrypt(&arena, expr, &[data, key], &chunk_len_1()).unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();

        let cipher = super::super::common::latin1_string_to_bytes(out.value(0)).unwrap();
        let plain = super::super::common::aes_decrypt_raw(
            super::super::common::AesMode::Aes128Ecb,
            &cipher,
            b"starrocks-key",
            None,
            None,
        )
        .unwrap();

        assert_eq!(plain, b"hello world");
    }
}
