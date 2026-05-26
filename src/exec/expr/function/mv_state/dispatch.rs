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
    for (name, canonical) in MV_STATE_FUNCTIONS {
        map.insert(
            *name,
            crate::exec::expr::function::FunctionKind::MvState(canonical),
        );
    }
}

pub fn metadata(name: &str) -> Option<FunctionMeta> {
    MV_STATE_METADATA.iter().find(|m| m.name == name).copied()
}

pub fn eval_mv_state_function(
    name: &str,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let canonical = MV_STATE_FUNCTIONS
        .iter()
        .find_map(|(alias, target)| (*alias == name).then_some(*target))
        .unwrap_or(name);

    match canonical {
        "count_state_union" => super::count::eval_count_state_union(arena, expr, args, chunk),
        "count_state_visible" => super::count::eval_count_state_visible(arena, expr, args, chunk),
        "count_distinct_state_union" => {
            super::count_distinct::eval_count_distinct_state_union(arena, expr, args, chunk)
        }
        "count_distinct_state_visible" => {
            super::count_distinct::eval_count_distinct_state_visible(arena, expr, args, chunk)
        }
        "approx_count_distinct_state_union" => {
            super::approx_count_distinct::eval_approx_count_distinct_state_union(
                arena, expr, args, chunk,
            )
        }
        "avg_state_union" => super::avg::eval_avg_state_union(arena, expr, args, chunk),
        "avg_state_visible" => super::avg::eval_avg_state_visible(arena, expr, args, chunk),
        "sum_state_union" => super::sum::eval_sum_state_union(arena, expr, args, chunk),
        "sum_state_visible" => super::sum::eval_sum_state_visible(arena, expr, args, chunk),
        "min_state_union" => super::min_max::eval_min_state_union(arena, expr, args, chunk),
        "min_state_visible" => super::min_max::eval_min_state_visible(arena, expr, args, chunk),
        "max_state_union" => super::min_max::eval_max_state_union(arena, expr, args, chunk),
        "max_state_visible" => super::min_max::eval_max_state_visible(arena, expr, args, chunk),
        "bool_or_state_union" => {
            super::bool_or_and::eval_bool_or_state_union(arena, expr, args, chunk)
        }
        "bool_or_state_visible" => {
            super::bool_or_and::eval_bool_or_state_visible(arena, expr, args, chunk)
        }
        "bool_and_state_union" => {
            super::bool_or_and::eval_bool_and_state_union(arena, expr, args, chunk)
        }
        "bool_and_state_visible" => {
            super::bool_or_and::eval_bool_and_state_visible(arena, expr, args, chunk)
        }
        other => Err(format!("unsupported mv_state function: {}", other)),
    }
}

static MV_STATE_FUNCTIONS: &[(&str, &str)] = &[
    ("count_state_union", "count_state_union"),
    ("count_state_visible", "count_state_visible"),
    ("count_distinct_state_union", "count_distinct_state_union"),
    (
        "count_distinct_state_visible",
        "count_distinct_state_visible",
    ),
    (
        "approx_count_distinct_state_union",
        "approx_count_distinct_state_union",
    ),
    ("avg_state_union", "avg_state_union"),
    ("avg_state_visible", "avg_state_visible"),
    ("sum_state_union", "sum_state_union"),
    ("sum_state_visible", "sum_state_visible"),
    ("min_state_union", "min_state_union"),
    ("min_state_visible", "min_state_visible"),
    ("max_state_union", "max_state_union"),
    ("max_state_visible", "max_state_visible"),
    ("bool_or_state_union", "bool_or_state_union"),
    ("bool_or_state_visible", "bool_or_state_visible"),
    ("bool_and_state_union", "bool_and_state_union"),
    ("bool_and_state_visible", "bool_and_state_visible"),
];

static MV_STATE_METADATA: &[FunctionMeta] = &[
    FunctionMeta {
        name: "count_state_union",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "count_state_visible",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "count_distinct_state_union",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "count_distinct_state_visible",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "approx_count_distinct_state_union",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "avg_state_union",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "avg_state_visible",
        min_args: 1,
        max_args: 2,
    },
    FunctionMeta {
        name: "sum_state_union",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "sum_state_visible",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "min_state_union",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "min_state_visible",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "max_state_union",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "max_state_visible",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "bool_or_state_union",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "bool_or_state_visible",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "bool_and_state_union",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "bool_and_state_visible",
        min_args: 1,
        max_args: 1,
    },
];
