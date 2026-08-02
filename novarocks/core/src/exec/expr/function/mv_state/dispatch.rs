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
        "state_all_zero" => super::count::eval_state_all_zero(arena, expr, args, chunk),
        "mv_group_row_id" => eval_mv_group_row_id(arena, expr, args, chunk),
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
        "approx_count_distinct_state_visible" => {
            super::approx_count_distinct::eval_approx_count_distinct_state_visible(
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

fn eval_mv_group_row_id(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.is_empty() {
        return Err("mv_group_row_id expects at least 1 argument, got 0".to_string());
    }
    let columns = args
        .iter()
        .map(|arg| arena.eval(*arg, chunk))
        .collect::<Result<Vec<_>, _>>()?;
    crate::mv::aggregate_state::mv_agg_state::aggregate_group_row_id_array(&columns)
}

static MV_STATE_FUNCTIONS: &[(&str, &str)] = &[
    ("count_state_union", "count_state_union"),
    ("count_state_visible", "count_state_visible"),
    ("state_all_zero", "state_all_zero"),
    ("mv_group_row_id", "mv_group_row_id"),
    ("count_distinct_state_union", "count_distinct_state_union"),
    (
        "count_distinct_state_visible",
        "count_distinct_state_visible",
    ),
    (
        "approx_count_distinct_state_union",
        "approx_count_distinct_state_union",
    ),
    (
        "approx_count_distinct_state_visible",
        "approx_count_distinct_state_visible",
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
        name: "state_all_zero",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "mv_group_row_id",
        min_args: 1,
        max_args: usize::MAX,
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
        name: "approx_count_distinct_state_visible",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "avg_state_union",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "avg_state_visible",
        min_args: 1,
        max_args: 3,
    },
    FunctionMeta {
        name: "sum_state_union",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "sum_state_visible",
        min_args: 1,
        max_args: 2,
    },
    FunctionMeta {
        name: "min_state_union",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "min_state_visible",
        min_args: 1,
        max_args: 2,
    },
    FunctionMeta {
        name: "max_state_union",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "max_state_visible",
        min_args: 1,
        max_args: 2,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, BinaryArray, Decimal128Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::common::ids::SlotId;
    use crate::exec::chunk::Chunk;
    use crate::exec::expr::function::{FunctionKind, function_metadata, lookup_function};
    use crate::exec::expr::{ExprNode, LiteralValue};

    #[test]
    fn state_all_zero_is_registered_as_mv_state_function() {
        assert_eq!(
            lookup_function("state_all_zero"),
            Some(FunctionKind::MvState("state_all_zero"))
        );

        let direct_meta = metadata("state_all_zero").unwrap();
        assert_eq!(direct_meta.min_args, 1);
        assert_eq!(direct_meta.max_args, 1);

        let registry_meta = function_metadata(FunctionKind::MvState("state_all_zero"));
        assert_eq!(registry_meta.name, "state_all_zero");
        assert_eq!(registry_meta.min_args, 1);
        assert_eq!(registry_meta.max_args, 1);
    }

    #[test]
    fn mv_group_row_id_matches_aggregate_state_physical_row_ids() {
        assert_eq!(
            lookup_function("mv_group_row_id"),
            Some(FunctionKind::MvState("mv_group_row_id"))
        );

        let mut arena = ExprArena::default();
        let k1 = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
        let k2 = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Utf8);
        let expr = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::MvState("mv_group_row_id"),
                args: vec![k1, k2],
            },
            DataType::Utf8,
        );
        let chunk = two_key_chunk();

        let out = arena.eval(expr, &chunk).unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        let expected =
            crate::mv::aggregate_state::mv_agg_state::aggregate_group_row_id_array(chunk.columns())
                .unwrap();
        let expected = expected.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(out.len(), expected.len());
        for row in 0..out.len() {
            assert_eq!(out.value(row), expected.value(row));
        }
    }

    #[test]
    fn avg_state_visible_three_args_keeps_decimal_scale() {
        let mut arena = ExprArena::default();
        let state = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Binary);
        let scale = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(4)), DataType::Int64);
        let witness = arena.push_typed(
            ExprNode::Literal(LiteralValue::Null),
            DataType::Decimal128(38, 10),
        );
        let expr = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::MvState("avg_state_visible"),
                args: vec![state, scale, witness],
            },
            DataType::Decimal128(38, 10),
        );
        let state = crate::mv::aggregate_state::state_codec::encode_sum_decimal128(2, 300_000);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "state",
                DataType::Binary,
                false,
            )])),
            vec![Arc::new(BinaryArray::from(vec![Some(state.as_slice())])) as ArrayRef],
        )
        .unwrap();
        let schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1)],
        )
        .unwrap();

        let out = arena
            .eval(expr, &Chunk::new_with_chunk_schema(batch, schema))
            .unwrap();
        let out = out.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(out.value(0), 150_000_000_000);
    }

    fn two_key_chunk() -> Chunk {
        let k1 = Arc::new(Int64Array::from(vec![Some(10), None, Some(10)])) as ArrayRef;
        let k2 = Arc::new(StringArray::from(vec![Some("a"), Some("a"), None])) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![
            Field::new("k1", DataType::Int64, true),
            Field::new("k2", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![k1, k2]).unwrap();
        let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1), SlotId::new(2)],
        )
        .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }
}
