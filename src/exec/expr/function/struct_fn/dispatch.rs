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
    for (name, canonical) in STRUCT_FUNCTIONS {
        map.insert(
            *name,
            crate::exec::expr::function::FunctionKind::StructFn(*canonical),
        );
    }
}

pub fn metadata(name: &str) -> Option<FunctionMeta> {
    STRUCT_METADATA.iter().find(|m| m.name == name).copied()
}

pub fn eval_struct_function(
    name: &str,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let canonical = STRUCT_FUNCTIONS
        .iter()
        .find_map(|(alias, target)| (*alias == name).then_some(*target))
        .unwrap_or(name);

    match canonical {
        "row" => super::struct_func::eval_row(arena, expr, args, chunk),
        "named_struct" => super::struct_func::eval_named_struct(arena, expr, args, chunk),
        "subfield" => super::subfield::eval_subfield(arena, expr, args, chunk),
        other => Err(format!("unsupported struct function: {}", other)),
    }
}

static STRUCT_FUNCTIONS: &[(&str, &str)] = &[
    ("row", "row"),
    ("struct", "row"),
    ("named_struct", "named_struct"),
];

static STRUCT_METADATA: &[FunctionMeta] = &[
    FunctionMeta {
        name: "row",
        min_args: 1,
        max_args: usize::MAX,
    },
    FunctionMeta {
        name: "named_struct",
        min_args: 2,
        max_args: usize::MAX,
    },
    FunctionMeta {
        name: "subfield",
        min_args: 2,
        max_args: 2,
    },
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::expr::function::FunctionKind;
    use std::collections::HashMap;

    #[test]
    fn test_struct_register() {
        let mut m = HashMap::new();
        register(&mut m);
        assert_eq!(m.get("row"), Some(&FunctionKind::StructFn("row")));
        assert_eq!(m.get("struct"), Some(&FunctionKind::StructFn("row")));
    }
}
