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
            crate::exec::expr::function::FunctionKind::Map(canonical),
        );
    }
}

pub fn metadata(name: &str) -> Option<FunctionMeta> {
    MAP_METADATA.iter().find(|m| m.name == name).copied()
}

pub fn eval_map_function<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    name: &str,
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let canonical = MAP_FUNCTIONS
        .iter()
        .find_map(|(alias, target)| (*alias == name).then_some(*target))
        .unwrap_or(name);

    match canonical {
        "map" => Ok((super::map_literal::eval_map_literal(arena, expr, args, chunk))?),
        "map_from_arrays" => {
            Ok((super::map_from_arrays::eval_map_from_arrays(arena, expr, args, chunk))?)
        }
        "arrays_zip" => Ok((super::arrays_zip::eval_arrays_zip(arena, expr, args, chunk))?),
        "map_size" => Ok((super::map_size::eval_map_size(arena, expr, args, chunk))?),
        "map_keys" => Ok((super::map_keys::eval_map_keys(arena, expr, args, chunk))?),
        "map_values" => Ok((super::map_values::eval_map_values(arena, expr, args, chunk))?),
        "map_entries" => Ok((super::map_entries::eval_map_entries(arena, expr, args, chunk))?),
        "map_filter" => Ok((super::map_filter::eval_map_filter(arena, expr, args, chunk))?),
        "distinct_map_keys" => {
            Ok((super::distinct_map_keys::eval_distinct_map_keys(arena, expr, args, chunk))?)
        }
        "map_concat" => Ok((super::map_concat::eval_map_concat(arena, expr, args, chunk))?),
        "map_apply" => Ok((super::map_apply::eval_map_apply(arena, expr, args, chunk))?),
        "__map_element_at" | "element_at" => {
            Ok((super::element_at::eval_element_at(arena, expr, args, chunk))?)
        }
        "cardinality" => Ok((super::cardinality::eval_cardinality(arena, expr, args, chunk))?),
        other => Err((format!("unsupported map function: {}", other)).into()),
    }
}

static MAP_FUNCTIONS: &[(&str, &str)] = &[
    ("map", "map"),
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
    ("__map_element_at", "element_at"),
    ("element_at", "element_at"),
    ("cardinality", "cardinality"),
];

static MAP_METADATA: &[FunctionMeta] = &[
    FunctionMeta {
        name: "map",
        min_args: 0,
        max_args: usize::MAX,
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
        name: "__map_element_at",
        min_args: 2,
        max_args: 3,
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
