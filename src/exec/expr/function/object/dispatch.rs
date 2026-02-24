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
    for (name, canonical) in OBJECT_FUNCTIONS {
        map.insert(
            *name,
            crate::exec::expr::function::FunctionKind::Object(*canonical),
        );
    }
}

pub fn metadata(name: &str) -> Option<FunctionMeta> {
    OBJECT_METADATA.iter().find(|m| m.name == name).copied()
}

pub fn eval_object_function(
    name: &str,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let canonical = OBJECT_FUNCTIONS
        .iter()
        .find_map(|(alias, target)| (*alias == name).then_some(*target))
        .unwrap_or(name);

    match canonical {
        "array_to_bitmap" => super::array_to_bitmap::eval_array_to_bitmap(arena, expr, args, chunk),
        "bitmap_empty" => super::bitmap_functions::eval_bitmap_empty(arena, expr, args, chunk),
        "bitmap_from_string" => {
            super::bitmap_functions::eval_bitmap_from_string(arena, expr, args, chunk)
        }
        "bitmap_count" => super::bitmap_functions::eval_bitmap_count(arena, expr, args, chunk),
        "bitmap_min" => super::bitmap_functions::eval_bitmap_min(arena, expr, args, chunk),
        "bitmap_max" => super::bitmap_functions::eval_bitmap_max(arena, expr, args, chunk),
        "bitmap_and" => super::bitmap_functions::eval_bitmap_and(arena, expr, args, chunk),
        "bitmap_has_any" => super::bitmap_functions::eval_bitmap_has_any(arena, expr, args, chunk),
        "sub_bitmap" => super::bitmap_functions::eval_sub_bitmap(arena, expr, args, chunk),
        "bitmap_subset_limit" => {
            super::bitmap_functions::eval_bitmap_subset_limit(arena, expr, args, chunk)
        }
        "bitmap_subset_in_range" => {
            super::bitmap_functions::eval_bitmap_subset_in_range(arena, expr, args, chunk)
        }
        "bitmap_to_binary" => {
            super::bitmap_functions::eval_bitmap_to_binary(arena, expr, args, chunk)
        }
        "bitmap_from_binary" => {
            super::bitmap_functions::eval_bitmap_from_binary(arena, expr, args, chunk)
        }
        "bitmap_to_base64" => {
            super::bitmap_functions::eval_bitmap_to_base64(arena, expr, args, chunk)
        }
        "bitmap_to_array" => super::bitmap_to_array::eval_bitmap_to_array(arena, expr, args, chunk),
        "bitmap_to_string" => {
            super::bitmap_to_string::eval_bitmap_to_string(arena, expr, args, chunk)
        }
        "hll_empty" => super::hll_codec::eval_hll_empty(arena, expr, args, chunk),
        "hll_serialize" => super::hll_codec::eval_hll_serialize(arena, expr, args, chunk),
        "hll_deserialize" => super::hll_codec::eval_hll_deserialize(arena, expr, args, chunk),
        "hll_cardinality" => super::hll_codec::eval_hll_cardinality(arena, expr, args, chunk),
        "to_bitmap" => super::to_bitmap::eval_to_bitmap(arena, expr, args, chunk),
        "hll_hash" => super::hll_hash::eval_hll_hash(arena, expr, args, chunk),
        "ds_hll_count_distinct_state" => super::hll_hash::eval_hll_hash(arena, expr, args, chunk),
        "json_object" => super::json_object::eval_json_object(arena, expr, args, chunk),
        other => Err(format!("unsupported object function: {}", other)),
    }
}

static OBJECT_FUNCTIONS: &[(&str, &str)] = &[
    ("array_to_bitmap", "array_to_bitmap"),
    ("bitmap_empty", "bitmap_empty"),
    ("bitmap_from_string", "bitmap_from_string"),
    ("bitmap_count", "bitmap_count"),
    ("bitmap_min", "bitmap_min"),
    ("bitmap_max", "bitmap_max"),
    ("bitmap_and", "bitmap_and"),
    ("bitmap_has_any", "bitmap_has_any"),
    ("sub_bitmap", "sub_bitmap"),
    ("bitmap_subset_limit", "bitmap_subset_limit"),
    ("bitmap_subset_in_range", "bitmap_subset_in_range"),
    ("bitmap_to_binary", "bitmap_to_binary"),
    ("bitmap_from_binary", "bitmap_from_binary"),
    ("bitmap_to_base64", "bitmap_to_base64"),
    ("bitmap_to_array", "bitmap_to_array"),
    ("bitmap_to_string", "bitmap_to_string"),
    ("hll_empty", "hll_empty"),
    ("hll_serialize", "hll_serialize"),
    ("hll_deserialize", "hll_deserialize"),
    ("hll_cardinality", "hll_cardinality"),
    ("to_bitmap", "to_bitmap"),
    ("hll_hash", "hll_hash"),
    ("ds_hll_count_distinct_state", "ds_hll_count_distinct_state"),
    ("json_object", "json_object"),
    // Stream load historical alias in StarRocks FE.
    ("hll_hash1", "hll_hash"),
];

static OBJECT_METADATA: &[FunctionMeta] = &[
    FunctionMeta {
        name: "array_to_bitmap",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "bitmap_empty",
        min_args: 0,
        max_args: 0,
    },
    FunctionMeta {
        name: "bitmap_from_string",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "bitmap_count",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "bitmap_min",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "bitmap_max",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "bitmap_and",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "bitmap_has_any",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "sub_bitmap",
        min_args: 3,
        max_args: 3,
    },
    FunctionMeta {
        name: "bitmap_subset_limit",
        min_args: 3,
        max_args: 3,
    },
    FunctionMeta {
        name: "bitmap_subset_in_range",
        min_args: 3,
        max_args: 3,
    },
    FunctionMeta {
        name: "bitmap_to_binary",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "bitmap_from_binary",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "bitmap_to_base64",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "bitmap_to_array",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "bitmap_to_string",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "hll_empty",
        min_args: 0,
        max_args: 0,
    },
    FunctionMeta {
        name: "hll_serialize",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "hll_deserialize",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "hll_cardinality",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "to_bitmap",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "hll_hash",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "ds_hll_count_distinct_state",
        min_args: 1,
        max_args: 3,
    },
    FunctionMeta {
        name: "json_object",
        min_args: 1,
        max_args: usize::MAX,
    },
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::expr::function::FunctionKind;

    #[test]
    fn test_register_object_functions() {
        let mut m = HashMap::new();
        register(&mut m);
        assert_eq!(m.get("hll_empty"), Some(&FunctionKind::Object("hll_empty")));
        assert_eq!(
            m.get("hll_serialize"),
            Some(&FunctionKind::Object("hll_serialize"))
        );
        assert_eq!(
            m.get("hll_deserialize"),
            Some(&FunctionKind::Object("hll_deserialize"))
        );
        assert_eq!(
            m.get("hll_cardinality"),
            Some(&FunctionKind::Object("hll_cardinality"))
        );
        assert_eq!(m.get("to_bitmap"), Some(&FunctionKind::Object("to_bitmap")));
        assert_eq!(m.get("hll_hash"), Some(&FunctionKind::Object("hll_hash")));
        assert_eq!(m.get("hll_hash1"), Some(&FunctionKind::Object("hll_hash")));
        assert_eq!(
            m.get("bitmap_to_string"),
            Some(&FunctionKind::Object("bitmap_to_string"))
        );
        assert_eq!(
            m.get("array_to_bitmap"),
            Some(&FunctionKind::Object("array_to_bitmap"))
        );
        assert_eq!(
            m.get("bitmap_to_array"),
            Some(&FunctionKind::Object("bitmap_to_array"))
        );
        assert_eq!(
            m.get("ds_hll_count_distinct_state"),
            Some(&FunctionKind::Object("ds_hll_count_distinct_state"))
        );
    }
}
