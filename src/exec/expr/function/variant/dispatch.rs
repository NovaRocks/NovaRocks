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
    for (name, canonical) in VARIANT_FUNCTIONS {
        map.insert(
            *name,
            crate::exec::expr::function::FunctionKind::Variant(*canonical),
        );
    }
}

pub fn metadata(name: &str) -> Option<FunctionMeta> {
    VARIANT_METADATA.iter().find(|m| m.name == name).copied()
}

pub fn eval_variant_function(
    name: &str,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let canonical = VARIANT_FUNCTIONS
        .iter()
        .find_map(|(alias, target)| (*alias == name).then_some(*target))
        .unwrap_or(name);
    match canonical {
        "parse_json" => super::parse_json::eval_parse_json(arena, expr, args, chunk),
        "json_query" => super::get_variant::eval_json_query(arena, expr, args, chunk),
        "variant_query" => super::get_variant::eval_variant_query(arena, expr, args, chunk),
        "get_variant_bool" => super::get_variant::eval_get_variant_bool(arena, expr, args, chunk),
        "get_variant_int" => super::get_variant::eval_get_variant_int(arena, expr, args, chunk),
        "get_variant_double" => {
            super::get_variant::eval_get_variant_double(arena, expr, args, chunk)
        }
        "get_variant_string" => {
            super::get_variant::eval_get_variant_string(arena, expr, args, chunk)
        }
        "get_json_string" => super::get_variant::eval_get_variant_string(arena, expr, args, chunk),
        "get_variant_date" => super::get_variant::eval_get_variant_date(arena, expr, args, chunk),
        "get_variant_datetime" => {
            super::get_variant::eval_get_variant_datetime(arena, expr, args, chunk)
        }
        "get_variant_time" => super::get_variant::eval_get_variant_time(arena, expr, args, chunk),
        "json_keys" => super::json_keys::eval_json_keys(arena, expr, args, chunk),
        "variant_typeof" => super::variant_typeof::eval_variant_typeof(arena, expr, args, chunk),
        other => Err(format!("unsupported variant function: {}", other)),
    }
}

static VARIANT_FUNCTIONS: &[(&str, &str)] = &[
    ("parse_json", "parse_json"),
    ("variant_query", "variant_query"),
    ("json_query", "json_query"),
    ("get_variant_bool", "get_variant_bool"),
    ("get_variant_int", "get_variant_int"),
    ("get_json_int", "get_variant_int"),
    ("get_variant_double", "get_variant_double"),
    ("get_variant_string", "get_variant_string"),
    ("get_json_string", "get_variant_string"),
    ("get_json_object", "get_variant_string"),
    ("get_variant_date", "get_variant_date"),
    ("get_variant_datetime", "get_variant_datetime"),
    ("get_variant_time", "get_variant_time"),
    ("json_keys", "json_keys"),
    ("variant_typeof", "variant_typeof"),
];

static VARIANT_METADATA: &[FunctionMeta] = &[
    FunctionMeta {
        name: "parse_json",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "variant_query",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "json_query",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "get_variant_bool",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "get_variant_int",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "get_json_int",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "get_variant_double",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "get_variant_string",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "get_json_string",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "get_variant_date",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "get_variant_datetime",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "get_variant_time",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "json_keys",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "variant_typeof",
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
    fn test_register_variant_functions() {
        let mut m = HashMap::new();
        register(&mut m);
        assert_eq!(
            m.get("json_query"),
            Some(&FunctionKind::Variant("json_query"))
        );
        assert_eq!(
            m.get("variant_typeof"),
            Some(&FunctionKind::Variant("variant_typeof"))
        );
        assert_eq!(
            m.get("get_json_object"),
            Some(&FunctionKind::Variant("get_variant_string"))
        );
        assert_eq!(
            m.get("get_json_int"),
            Some(&FunctionKind::Variant("get_variant_int"))
        );
    }
}
