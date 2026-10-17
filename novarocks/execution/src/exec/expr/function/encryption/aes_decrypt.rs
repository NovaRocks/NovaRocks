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
use novarocks_functions::builtin::aes_rows::{self, Operation, Row};
use novarocks_type_contract::ToBase64ByteSource;
pub fn eval_aes_decrypt<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    if args.len() != 2 && args.len() != 4 && args.len() != 5 {
        return Err(("aes_decrypt expects 2, 4, or 5 arguments".to_string()).into());
    }

    let source = match arena.node(args[0]) {
        Some(ExprNode::FunctionCall {
            kind: FunctionKind::Encryption(name),
            ..
        }) => ToBase64ByteSource::from_immediate_encryption_identity(Some(*name)),
        _ => ToBase64ByteSource::from_immediate_encryption_identity(None),
    };
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

    let two;
    let four;
    let five;
    let inputs = if args.len() == 2 {
        two = [src, key];
        &two[..]
    } else if args.len() == 4 {
        four = [src, key, iv.unwrap(), mode.unwrap()];
        &four[..]
    } else {
        five = [src, key, iv.unwrap(), mode.unwrap(), aad.unwrap()];
        &five[..]
    };
    let mut out = Vec::with_capacity(chunk.len());
    for row in 0..chunk.len() {
        let rows = [row; 5];
        match aes_rows::evaluate_row(Operation::Decrypt, inputs, &rows[..inputs.len()], source) {
            Row::Value(value) => out.push(value),
            Row::Data(_) => {
                return Err(
                    ("aes_decrypt: requires GCM mode to use AAD parameter".to_string()).into(),
                );
            }
        }
    }
    Ok((super::common::build_bytes_output_lossy(out, arena.data_type(expr)))?)
}
