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
use arrow::array::ArrayRef;

fn should_prefer_latin1_bytes(arena: &ExprArena, arg: ExprId) -> bool {
    matches!(
        arena.node(arg),
        Some(ExprNode::FunctionCall {
            kind: FunctionKind::Encryption(name),
            ..
        }) if matches!(*name, "from_base64" | "aes_encrypt" | "to_binary")
    )
}

fn decrypt_with_utf8_fallback(
    mode: super::common::AesMode,
    src: &super::common::OwnedBytesArray,
    row: usize,
    key: &[u8],
    iv: Option<&[u8]>,
    aad: Option<&[u8]>,
    prefer_latin1: bool,
) -> Option<Vec<u8>> {
    let utf8_bytes = src.bytes(row);
    let latin1 = src
        .utf8(row)
        .and_then(super::common::latin1_string_to_bytes);
    let primary = if prefer_latin1 {
        latin1.as_deref().unwrap_or(utf8_bytes)
    } else {
        utf8_bytes
    };

    let mut result = super::common::aes_decrypt_raw(mode, primary, key, iv, aad);
    if result.is_some() {
        return result;
    }

    let fallback = if prefer_latin1 {
        Some(utf8_bytes)
    } else {
        latin1.as_deref()
    };
    if let Some(fallback) = fallback {
        if fallback != primary {
            result = super::common::aes_decrypt_raw(mode, fallback, key, iv, aad);
        }
    }

    result
}

pub fn eval_aes_decrypt(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() != 2 && args.len() != 4 && args.len() != 5 {
        return Err("aes_decrypt expects 2, 4, or 5 arguments".to_string());
    }

    let prefer_latin1 = should_prefer_latin1_bytes(arena, args[0]);
    let src = super::common::to_owned_bytes_array(arena.eval(args[0], chunk)?, "aes_decrypt", 0)?;
    let key = super::common::to_owned_bytes_array(arena.eval(args[1], chunk)?, "aes_decrypt", 1)?;

    let iv = if args.len() >= 4 {
        Some(super::common::to_owned_bytes_array(
            arena.eval(args[2], chunk)?,
            "aes_decrypt",
            2,
        )?)
    } else {
        None
    };

    let mode = if args.len() >= 4 {
        Some(super::common::to_owned_bytes_array(
            arena.eval(args[3], chunk)?,
            "aes_decrypt",
            3,
        )?)
    } else {
        None
    };

    let aad = if args.len() == 5 {
        Some(super::common::to_owned_bytes_array(
            arena.eval(args[4], chunk)?,
            "aes_decrypt",
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

        let src_bytes = src.bytes(row);
        let key_bytes = key.bytes(row);
        if src_bytes.is_empty() || key_bytes.is_empty() {
            out.push(None);
            continue;
        }

        if args.len() == 2 {
            out.push(decrypt_with_utf8_fallback(
                super::common::AesMode::Aes128Ecb,
                &src,
                row,
                key_bytes,
                None,
                None,
                prefer_latin1,
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

        out.push(decrypt_with_utf8_fallback(
            mode,
            &src,
            row,
            key_bytes,
            iv_bytes,
            aad_bytes,
            prefer_latin1,
        ));
    }

    super::common::build_bytes_output_lossy(out, arena.data_type(expr))
}

#[cfg(test)]
mod tests {
    use super::eval_aes_decrypt;
    use crate::exec::expr::function::FunctionKind;
    use crate::exec::expr::function::encryption::test_utils::{
        chunk_len_1, literal_string, typed_null,
    };
    use crate::exec::expr::{ExprArena, ExprNode};
    use arrow::array::StringArray;
    use arrow::datatypes::DataType;

    #[test]
    fn test_aes_decrypt_with_latin1_ciphertext() {
        let cipher = super::super::common::aes_encrypt_raw(
            super::super::common::AesMode::Aes128Ecb,
            b"hello",
            b"k",
            None,
            None,
        )
        .unwrap();
        let cipher_latin1: String = cipher.iter().map(|b| char::from(*b)).collect();

        let mut arena = ExprArena::default();
        let expr = typed_null(&mut arena, DataType::Utf8);
        let cipher_expr = literal_string(&mut arena, &cipher_latin1);
        let key_expr = literal_string(&mut arena, "k");

        let out = eval_aes_decrypt(&arena, expr, &[cipher_expr, key_expr], &chunk_len_1()).unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out.value(0), "hello");
    }

    #[test]
    fn test_aes_decrypt_prefers_latin1_bytes_for_from_base64_input() {
        let mut arena = ExprArena::default();
        let expr = typed_null(&mut arena, DataType::Utf8);
        let cipher_b64 = literal_string(&mut arena, "3wGmUeFO4/Ex");
        let from_b64 = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Encryption("from_base64"),
                args: vec![cipher_b64],
            },
            DataType::Utf8,
        );
        let key_expr = literal_string(&mut arena, "F3229A0B371ED2D9441B830D21A390C3");
        let iv_expr = literal_string(&mut arena, "");
        let mode_expr = literal_string(&mut arena, "AES_128_CFB");

        let out = eval_aes_decrypt(
            &arena,
            expr,
            &[from_b64, key_expr, iv_expr, mode_expr],
            &chunk_len_1(),
        )
        .unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out.value(0), "starrocks");
    }
}
