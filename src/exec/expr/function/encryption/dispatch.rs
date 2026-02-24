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
            crate::exec::expr::function::FunctionKind::Encryption(*canonical),
        );
    }
}

pub fn metadata(name: &str) -> Option<FunctionMeta> {
    ENCRYPTION_METADATA.iter().find(|m| m.name == name).copied()
}

pub fn eval_encryption_function(
    name: &str,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let canonical = ENCRYPTION_FUNCTIONS
        .iter()
        .find_map(|(alias, target)| (*alias == name).then_some(*target))
        .unwrap_or(name);

    match canonical {
        "aes_encrypt" => super::aes_encrypt::eval_aes_encrypt(arena, expr, args, chunk),
        "aes_decrypt" => super::aes_decrypt::eval_aes_decrypt(arena, expr, args, chunk),
        "encode_fingerprint_sha256" => {
            super::encode_fingerprint_sha256::eval_encode_fingerprint_sha256(
                arena, expr, args, chunk,
            )
        }
        "encode_sort_key" => super::encode_sort_key::eval_encode_sort_key(arena, expr, args, chunk),
        "from_base64" => super::from_base64::eval_from_base64(arena, expr, args, chunk),
        "from_binary" => super::from_binary::eval_from_binary(arena, expr, args, chunk),
        "md5" => super::md5::eval_md5(arena, expr, args, chunk),
        "md5sum" => super::md5sum::eval_md5sum(arena, expr, args, chunk),
        "md5sum_numeric" => super::md5sum_numeric::eval_md5sum_numeric(arena, expr, args, chunk),
        "sha2" => super::sha2::eval_sha2(arena, expr, args, chunk),
        "sm3" => super::sm3::eval_sm3(arena, expr, args, chunk),
        "to_base64" => super::to_base64::eval_to_base64(arena, expr, args, chunk),
        "to_binary" => super::to_binary::eval_to_binary(arena, expr, args, chunk),
        other => Err(format!("unsupported encryption function: {}", other)),
    }
}

static ENCRYPTION_FUNCTIONS: &[(&str, &str)] = &[
    ("aes_decrypt", "aes_decrypt"),
    ("aes_encrypt", "aes_encrypt"),
    ("encode_fingerprint_sha256", "encode_fingerprint_sha256"),
    ("encode_row_id", "encode_fingerprint_sha256"),
    ("encode_sort_key", "encode_sort_key"),
    ("base64_decode_binary", "from_base64"),
    ("base64_decode_string", "from_base64"),
    ("from_base64", "from_base64"),
    ("from_binary", "from_binary"),
    ("md5", "md5"),
    ("md5sum", "md5sum"),
    ("md5sum_numeric", "md5sum_numeric"),
    ("sha2", "sha2"),
    ("sm3", "sm3"),
    ("to_base64", "to_base64"),
    ("to_binary", "to_binary"),
];

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::expr::function::FunctionKind;

    #[test]
    fn test_register_encryption_functions() {
        let mut m = HashMap::new();
        register(&mut m);
        assert_eq!(m.get("md5"), Some(&FunctionKind::Encryption("md5")));
        assert_eq!(
            m.get("encode_fingerprint_sha256"),
            Some(&FunctionKind::Encryption("encode_fingerprint_sha256"))
        );
        assert_eq!(
            m.get("encode_sort_key"),
            Some(&FunctionKind::Encryption("encode_sort_key"))
        );
        assert_eq!(
            m.get("base64_decode_binary"),
            Some(&FunctionKind::Encryption("from_base64"))
        );
    }
}
