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
    for (name, canonical) in ARRAY_FUNCTIONS {
        map.insert(
            *name,
            crate::exec::expr::function::FunctionKind::Array(*canonical),
        );
    }
}

pub fn metadata(name: &str) -> Option<FunctionMeta> {
    ARRAY_METADATA.iter().find(|m| m.name == name).copied()
}

pub fn eval_array_function(
    name: &str,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let canonical = ARRAY_FUNCTIONS
        .iter()
        .find_map(|(alias, target)| (*alias == name).then_some(*target))
        .unwrap_or(name);

    match canonical {
        "all_match" => super::all_match::eval_all_match(arena, expr, args, chunk),
        "any_match" => super::any_match::eval_any_match(arena, expr, args, chunk),
        "array_append" => super::array_append::eval_array_append(arena, expr, args, chunk),
        "array_avg" => super::array_avg::eval_array_avg(arena, expr, args, chunk),
        "array_concat" => super::array_concat::eval_array_concat(arena, expr, args, chunk),
        "array_cum_sum" => super::array_cum_sum::eval_array_cum_sum(arena, expr, args, chunk),
        "array_length" => super::array_length::eval_array_length(arena, expr, args, chunk),
        "array_contains" => super::array_contains::eval_array_contains(arena, expr, args, chunk),
        "array_contains_all" => {
            super::array_contains_all::eval_array_contains_all(arena, expr, args, chunk)
        }
        "array_contains_seq" => {
            super::array_contains_seq::eval_array_contains_seq(arena, expr, args, chunk)
        }
        "array_distinct" => super::array_distinct::eval_array_distinct(arena, expr, args, chunk),
        "array_difference" => {
            super::array_difference::eval_array_difference(arena, expr, args, chunk)
        }
        "element_at" => super::element_at::eval_element_at(arena, expr, args, chunk),
        "array_filter" => super::array_filter::eval_array_filter(arena, expr, args, chunk),
        "array_flatten" => super::array_flatten::eval_array_flatten(arena, expr, args, chunk),
        "array_generate" => super::array_generate::eval_array_generate(arena, expr, args, chunk),
        "array_intersect" => super::array_intersect::eval_array_intersect(arena, expr, args, chunk),
        "array_join" => super::array_join::eval_array_join(arena, expr, args, chunk),
        "array_max" => super::array_max::eval_array_max(arena, expr, args, chunk),
        "array_min" => super::array_min::eval_array_min(arena, expr, args, chunk),
        "array_position" => super::array_position::eval_array_position(arena, expr, args, chunk),
        "array_repeat" => super::array_repeat::eval_array_repeat(arena, expr, args, chunk),
        "array_remove" => super::array_remove::eval_array_remove(arena, expr, args, chunk),
        "array_slice" => super::array_slice::eval_array_slice(arena, expr, args, chunk),
        "array_sort" => super::array_sort::eval_array_sort(arena, expr, args, chunk),
        "array_sort_lambda" => {
            super::array_sort_lambda::eval_array_sort_lambda(arena, expr, args, chunk)
        }
        "array_sortby" => super::array_sortby::eval_array_sortby(arena, expr, args, chunk),
        "array_top_n" => super::array_top_n::eval_array_top_n(arena, expr, args, chunk),
        "array_sum" => super::array_sum::eval_array_sum(arena, expr, args, chunk),
        "arrays_overlap" => super::arrays_overlap::eval_arrays_overlap(arena, expr, args, chunk),
        other => Err(format!("unsupported array function: {}", other)),
    }
}

static ARRAY_FUNCTIONS: &[(&str, &str)] = &[
    ("all_match", "all_match"),
    ("any_match", "any_match"),
    ("array_append", "array_append"),
    ("array_avg", "array_avg"),
    ("array_concat", "array_concat"),
    ("array_cum_sum", "array_cum_sum"),
    ("array_length", "array_length"),
    ("array_contains", "array_contains"),
    ("array_contains_all", "array_contains_all"),
    ("array_contains_seq", "array_contains_seq"),
    ("array_difference", "array_difference"),
    ("array_distinct", "array_distinct"),
    ("array_filter", "array_filter"),
    ("array_flatten", "array_flatten"),
    ("array_generate", "array_generate"),
    ("array_intersect", "array_intersect"),
    ("array_join", "array_join"),
    ("array_max", "array_max"),
    ("array_min", "array_min"),
    ("array_position", "array_position"),
    ("array_repeat", "array_repeat"),
    ("array_remove", "array_remove"),
    ("array_slice", "array_slice"),
    ("array_sort", "array_sort"),
    ("array_sort_lambda", "array_sort_lambda"),
    ("array_sortby", "array_sortby"),
    ("array_top_n", "array_top_n"),
    ("array_sum", "array_sum"),
    ("arrays_overlap", "arrays_overlap"),
];

static ARRAY_METADATA: &[FunctionMeta] = &[
    FunctionMeta {
        name: "all_match",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "any_match",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "array_append",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "array_avg",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "array_concat",
        min_args: 1,
        max_args: usize::MAX,
    },
    FunctionMeta {
        name: "array_cum_sum",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "array_length",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "array_contains",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "array_contains_all",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "array_contains_seq",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "array_difference",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "element_at",
        min_args: 2,
        max_args: 3,
    },
    FunctionMeta {
        name: "array_distinct",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "array_filter",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "array_flatten",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "array_generate",
        min_args: 1,
        max_args: 4,
    },
    FunctionMeta {
        name: "array_intersect",
        min_args: 2,
        max_args: usize::MAX,
    },
    FunctionMeta {
        name: "array_join",
        min_args: 2,
        max_args: 3,
    },
    FunctionMeta {
        name: "array_max",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "array_min",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "array_position",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "array_repeat",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "array_remove",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "array_slice",
        min_args: 2,
        max_args: 3,
    },
    FunctionMeta {
        name: "array_sort",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "array_sort_lambda",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "array_sortby",
        min_args: 2,
        max_args: usize::MAX,
    },
    FunctionMeta {
        name: "array_top_n",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "array_sum",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "arrays_overlap",
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
    fn test_register_array_functions() {
        let mut m = HashMap::new();
        register(&mut m);
        assert_eq!(
            m.get("array_length"),
            Some(&FunctionKind::Array("array_length"))
        );
        assert_eq!(
            m.get("array_contains"),
            Some(&FunctionKind::Array("array_contains"))
        );
    }

    #[test]
    fn test_array_metadata() {
        let meta = metadata("array_slice").unwrap();
        assert_eq!(meta.min_args, 2);
        assert_eq!(meta.max_args, 3);
    }
}
