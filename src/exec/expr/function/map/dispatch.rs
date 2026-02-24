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
    for (name, canonical) in MAP_FUNCTIONS {
        map.insert(
            *name,
            crate::exec::expr::function::FunctionKind::Map(*canonical),
        );
    }
}

pub fn metadata(name: &str) -> Option<FunctionMeta> {
    MAP_METADATA.iter().find(|m| m.name == name).copied()
}

pub fn eval_map_function(
    name: &str,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let canonical = MAP_FUNCTIONS
        .iter()
        .find_map(|(alias, target)| (*alias == name).then_some(*target))
        .unwrap_or(name);

    match canonical {
        "map_from_arrays" => super::map_from_arrays::eval_map_from_arrays(arena, expr, args, chunk),
        "arrays_zip" => super::arrays_zip::eval_arrays_zip(arena, expr, args, chunk),
        "map_size" => super::map_size::eval_map_size(arena, expr, args, chunk),
        "map_keys" => super::map_keys::eval_map_keys(arena, expr, args, chunk),
        "map_values" => super::map_values::eval_map_values(arena, expr, args, chunk),
        "map_entries" => super::map_entries::eval_map_entries(arena, expr, args, chunk),
        "map_filter" => super::map_filter::eval_map_filter(arena, expr, args, chunk),
        "distinct_map_keys" => {
            super::distinct_map_keys::eval_distinct_map_keys(arena, expr, args, chunk)
        }
        "map_concat" => super::map_concat::eval_map_concat(arena, expr, args, chunk),
        "map_apply" => super::map_apply::eval_map_apply(arena, expr, args, chunk),
        "element_at" => super::element_at::eval_element_at(arena, expr, args, chunk),
        "cardinality" => super::cardinality::eval_cardinality(arena, expr, args, chunk),
        other => Err(format!("unsupported map function: {}", other)),
    }
}

static MAP_FUNCTIONS: &[(&str, &str)] = &[
    ("map", "map_from_arrays"),
    ("map_from_arrays", "map_from_arrays"),
    ("arrays_zip", "arrays_zip"),
    ("map_size", "map_size"),
    ("map_keys", "map_keys"),
    ("map_values", "map_values"),
    ("map_entries", "map_entries"),
    ("map_filter", "map_filter"),
    ("distinct_map_keys", "distinct_map_keys"),
    ("map_concat", "map_concat"),
    ("map_apply", "map_apply"),
    ("transform_keys", "map_apply"),
    ("transform_values", "map_apply"),
    ("element_at", "element_at"),
    ("cardinality", "cardinality"),
];

static MAP_METADATA: &[FunctionMeta] = &[
    FunctionMeta {
        name: "map",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "map_from_arrays",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "arrays_zip",
        min_args: 1,
        max_args: usize::MAX,
    },
    FunctionMeta {
        name: "map_size",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "map_keys",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "map_values",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "map_entries",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "map_filter",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "distinct_map_keys",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "map_concat",
        min_args: 1,
        max_args: usize::MAX,
    },
    FunctionMeta {
        name: "map_apply",
        min_args: 2,
        max_args: usize::MAX,
    },
    FunctionMeta {
        name: "transform_keys",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "transform_values",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "element_at",
        min_args: 2,
        max_args: 3,
    },
    FunctionMeta {
        name: "cardinality",
        min_args: 1,
        max_args: 1,
    },
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::expr::function::FunctionKind;
    use std::collections::HashMap;

    #[test]
    fn test_register_map_functions() {
        let mut m = HashMap::new();
        register(&mut m);
        assert_eq!(m.get("map"), Some(&FunctionKind::Map("map_from_arrays")));
        assert_eq!(m.get("map_size"), Some(&FunctionKind::Map("map_size")));
        assert_eq!(
            m.get("transform_keys"),
            Some(&FunctionKind::Map("map_apply"))
        );
        assert_eq!(
            m.get("transform_values"),
            Some(&FunctionKind::Map("map_apply"))
        );
        assert_eq!(
            m.get("cardinality"),
            Some(&FunctionKind::Map("cardinality"))
        );
    }
}
