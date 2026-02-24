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
    for (name, canonical) in MATCHING_FUNCTIONS {
        map.insert(
            *name,
            crate::exec::expr::function::FunctionKind::Matching(*canonical),
        );
    }
}

pub fn metadata(name: &str) -> Option<FunctionMeta> {
    MATCHING_METADATA.iter().find(|m| m.name == name).copied()
}

pub fn eval_matching_function(
    name: &str,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let canonical = MATCHING_FUNCTIONS
        .iter()
        .find_map(|(alias, target)| (*alias == name).then_some(*target))
        .unwrap_or(name);

    match canonical {
        "ilike" => super::ilike::eval_ilike(arena, expr, args, chunk),
        "regexp" => super::regexp::eval_regexp(arena, expr, args, chunk),
        other => Err(format!("unsupported matching function: {}", other)),
    }
}

static MATCHING_FUNCTIONS: &[(&str, &str)] = &[
    ("ilike", "ilike"),
    ("regexp", "regexp"),
    ("rlike", "regexp"),
];

static MATCHING_METADATA: &[FunctionMeta] = &[
    FunctionMeta {
        name: "ilike",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "regexp",
        min_args: 2,
        max_args: 2,
    },
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::expr::function::FunctionKind;

    #[test]
    fn test_register_matching_functions() {
        let mut m = HashMap::new();
        register(&mut m);
        assert_eq!(m.get("ilike"), Some(&FunctionKind::Matching("ilike")));
        assert_eq!(m.get("rlike"), Some(&FunctionKind::Matching("regexp")));
    }
}
