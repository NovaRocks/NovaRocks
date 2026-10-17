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
use std::collections::HashMap;

#[derive(Clone, Copy)]
pub struct FunctionMeta {
    pub name: &'static str,
    pub min_args: usize,
    pub max_args: usize,
}

pub fn register(map: &mut HashMap<&'static str, crate::exec::expr::function::FunctionKind>) {
    for (name, canonical) in ENCRYPTION_FUNCTIONS {
        map.insert(
            *name,
            crate::exec::expr::function::FunctionKind::Encryption(canonical),
        );
    }
}

pub fn metadata(name: &str) -> Option<FunctionMeta> {
    ENCRYPTION_METADATA.iter().find(|m| m.name == name).copied()
}

pub fn eval_encryption_function<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    name: &str,
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let canonical = ENCRYPTION_FUNCTIONS
        .iter()
        .find_map(|(alias, target)| (*alias == name).then_some(*target))
        .unwrap_or(name);

    match canonical {
        "aes_encrypt" => Ok((super::aes_encrypt::eval_aes_encrypt(arena, expr, args, chunk))?),
        "aes_decrypt" => Ok((super::aes_decrypt::eval_aes_decrypt(arena, expr, args, chunk))?),
        "encode_fingerprint_sha256" => Ok(
            (super::encode_fingerprint_sha256::eval_encode_fingerprint_sha256(
                arena, expr, args, chunk,
            ))?,
        ),
        "encode_sort_key" => {
            Ok((super::encode_sort_key::eval_encode_sort_key(arena, expr, args, chunk))?)
        }
        "from_base64" => Ok((super::from_base64::eval_from_base64(arena, expr, args, chunk))?),
        "from_binary" => Ok((super::from_binary::eval_from_binary(arena, expr, args, chunk))?),
        "md5" => Ok((super::md5::eval_md5(arena, expr, args, chunk))?),
        "md5sum" => Ok((super::md5sum::eval_md5sum(arena, expr, args, chunk))?),
        "md5sum_numeric" => {
            Ok((super::md5sum_numeric::eval_md5sum_numeric(arena, expr, args, chunk))?)
        }
        "sha2" => Ok((super::sha2::eval_sha2(arena, expr, args, chunk))?),
        "sm3" => Ok((super::sm3::eval_sm3(arena, expr, args, chunk))?),
        "to_base64" => Ok((super::to_base64::eval_to_base64(arena, expr, args, chunk))?),
        "to_binary" => Ok((super::to_binary::eval_to_binary(arena, expr, args, chunk))?),
        other => Err((format!("unsupported encryption function: {}", other)).into()),
    }
}

use novarocks_type_contract::NATIVE_V1_ENCRYPTION_FUNCTIONS as ENCRYPTION_FUNCTIONS;

static ENCRYPTION_METADATA: &[FunctionMeta] = &[
    FunctionMeta {
        name: "aes_decrypt",
        min_args: 2,
        max_args: 5,
    },
    FunctionMeta {
        name: "aes_encrypt",
        min_args: 2,
        max_args: 5,
    },
    FunctionMeta {
        name: "encode_fingerprint_sha256",
        min_args: 1,
        max_args: usize::MAX,
    },
    FunctionMeta {
        name: "encode_sort_key",
        min_args: 1,
        max_args: usize::MAX,
    },
    FunctionMeta {
        name: "from_base64",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "from_binary",
        min_args: 1,
        max_args: 2,
    },
    FunctionMeta {
        name: "md5",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "md5sum",
        min_args: 1,
        max_args: usize::MAX,
    },
    FunctionMeta {
        name: "md5sum_numeric",
        min_args: 1,
        max_args: usize::MAX,
    },
    FunctionMeta {
        name: "sha2",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "sm3",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "to_base64",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "to_binary",
        min_args: 1,
        max_args: 2,
    },
];
