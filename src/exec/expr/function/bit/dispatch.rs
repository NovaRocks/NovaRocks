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
    for (name, canonical) in BIT_FUNCTIONS {
        map.insert(
            *name,
            crate::exec::expr::function::FunctionKind::Bit(*canonical),
        );
    }
}

pub fn metadata(name: &str) -> Option<FunctionMeta> {
    BIT_METADATA.iter().find(|m| m.name == name).copied()
}

pub fn eval_bit_function(
    name: &str,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let canonical = BIT_FUNCTIONS
        .iter()
        .find_map(|(alias, target)| (*alias == name).then_some(*target))
        .unwrap_or(name);

    match canonical {
        "bit_shift_left" => super::bit_ops::eval_bit_shift_left(arena, expr, args, chunk),
        "bit_shift_right" => super::bit_ops::eval_bit_shift_right(arena, expr, args, chunk),
        "bit_shift_right_logical" => {
            super::bit_ops::eval_bit_shift_right_logical(arena, expr, args, chunk)
        }
        "bitand" => super::bit_ops::eval_bitand(arena, expr, args, chunk),
        "bitnot" => super::bit_ops::eval_bitnot(arena, expr, args, chunk),
        "bitor" => super::bit_ops::eval_bitor(arena, expr, args, chunk),
        "bitxor" => super::bit_ops::eval_bitxor(arena, expr, args, chunk),
        "xx_hash3_128" => super::xx_hash3_128::eval_xx_hash3_128(arena, expr, args, chunk),
        other => Err(format!("unsupported bit function: {}", other)),
    }
}

static BIT_FUNCTIONS: &[(&str, &str)] = &[
    ("bit_shift_left", "bit_shift_left"),
    ("bit_shift_right", "bit_shift_right"),
    ("bit_shift_right_logical", "bit_shift_right_logical"),
    ("bitand", "bitand"),
    ("bitnot", "bitnot"),
    ("bitor", "bitor"),
    ("bitxor", "bitxor"),
    ("xx_hash3_128", "xx_hash3_128"),
];

static BIT_METADATA: &[FunctionMeta] = &[
    FunctionMeta {
        name: "bit_shift_left",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "bit_shift_right",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "bit_shift_right_logical",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "bitand",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "bitnot",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "bitor",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "bitxor",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "xx_hash3_128",
        min_args: 1,
        max_args: usize::MAX,
    },
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::expr::function::FunctionKind;

    #[test]
    fn test_register_bit_functions() {
        let mut m = HashMap::new();
        register(&mut m);
        assert_eq!(m.get("bitand"), Some(&FunctionKind::Bit("bitand")));
        assert_eq!(
            m.get("bit_shift_right_logical"),
            Some(&FunctionKind::Bit("bit_shift_right_logical"))
        );
        assert_eq!(
            m.get("xx_hash3_128"),
            Some(&FunctionKind::Bit("xx_hash3_128"))
        );
    }
}
