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
#![allow(unused_imports)]

use crate::common;
use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Int32Array, Int64Array, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks::common::ids::SlotId;
use novarocks::exec::chunk::{Chunk, ChunkSchema};
use novarocks::exec::expr::function::FunctionKind;
use novarocks::exec::expr::function::object::{
    eval_hll_hash, eval_object_function, eval_percentile_hash, eval_to_bitmap, register,
};
use novarocks::exec::expr::{ExprArena, ExprNode, LiteralValue};
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Shared helper for object tests
// ---------------------------------------------------------------------------

fn one_col_chunk(data_type: DataType, array: ArrayRef) -> Chunk {
    let field = Field::new("c1", data_type.clone(), true);
    let schema = Arc::new(Schema::new(vec![field]));
    let batch = RecordBatch::try_new(schema, vec![array]).expect("record batch");
    let chunk_schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[SlotId(1)])
            .expect("chunk schema");
    Chunk::new_with_chunk_schema(batch, chunk_schema)
}

// ---------------------------------------------------------------------------
// Tests migrated from object/dispatch.rs
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Tests migrated from object/json_object.rs
// ---------------------------------------------------------------------------

#[test]
fn test_json_value_from_utf8_json_number() {
    // Test via eval_object_function("json_object", ...)
    // json_object takes pairs of key-value literals
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr = common::typed_null(&mut arena, DataType::Utf8);
    let key = common::literal_string(&mut arena, "n");
    let val = common::literal_string(&mut arena, "23");

    // json_object("n", "23") should produce {"n": "23"} - the value is a string not a number
    // when passed as a literal string
    let out = eval_object_function("json_object", &arena, expr, &[key, val], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<StringArray>().unwrap();
    // Result should be a valid JSON object
    assert!(!out.is_null(0));
    let result = out.value(0);
    assert!(result.contains("n"));
}

// ---------------------------------------------------------------------------
// Tests migrated from object/hll_hash.rs
// ---------------------------------------------------------------------------

#[test]
fn hll_hash_encodes_explicit_hash_and_empty_for_null() {
    let mut arena = ExprArena::default();
    let arg = arena.push_typed(ExprNode::SlotId(SlotId(1)), DataType::Utf8);

    let input = Arc::new(StringArray::from(vec![Some("a"), None])) as ArrayRef;
    let chunk = one_col_chunk(DataType::Utf8, input);
    let out = eval_hll_hash(&arena, arg, &[arg], &chunk).expect("eval");
    let out = out.as_any().downcast_ref::<BinaryArray>().expect("binary");

    assert_eq!(out.value(0)[0], 1);
    assert_eq!(out.value(0).len(), 10);
    assert_eq!(out.value(1), &[0]);
}

#[test]
fn hll_hash_is_deterministic() {
    let mut arena = ExprArena::default();
    let arg = arena.push_typed(ExprNode::SlotId(SlotId(1)), DataType::Utf8);
    let expr = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Object("hll_hash"),
            args: vec![arg],
        },
        DataType::Binary,
    );

    let input = Arc::new(StringArray::from(vec![Some("x"), Some("x")])) as ArrayRef;
    let chunk = one_col_chunk(DataType::Utf8, input);
    let out = arena.eval(expr, &chunk).expect("eval");
    let out = out.as_any().downcast_ref::<BinaryArray>().expect("binary");

    assert_eq!(out.value(0), out.value(1));
}

// ---------------------------------------------------------------------------
// Tests migrated from object/to_bitmap.rs
// ---------------------------------------------------------------------------

#[test]
fn to_bitmap_encodes_single_values() {
    let mut arena = ExprArena::default();
    let arg = arena.push_typed(ExprNode::SlotId(SlotId(1)), DataType::Int64);
    let expr = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Object("to_bitmap"),
            args: vec![arg],
        },
        DataType::Binary,
    );

    let input = Arc::new(Int64Array::from(vec![Some(1), Some(2), None])) as ArrayRef;
    let chunk = one_col_chunk(DataType::Int64, input);
    let out = arena.eval(expr, &chunk).expect("eval");
    let out = out.as_any().downcast_ref::<BinaryArray>().expect("binary");

    assert_eq!(out.value(0), &[1, 1, 0, 0, 0]);
    assert_eq!(out.value(1), &[1, 2, 0, 0, 0]);
    assert!(out.is_null(2));
}

#[test]
fn to_bitmap_ignores_negative_values() {
    let mut arena = ExprArena::default();
    let arg = arena.push_typed(ExprNode::SlotId(SlotId(1)), DataType::Int64);
    let expr = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Object("to_bitmap"),
            args: vec![arg],
        },
        DataType::Binary,
    );

    let input = Arc::new(Int64Array::from(vec![Some(-1)])) as ArrayRef;
    let chunk = one_col_chunk(DataType::Int64, input);
    let out = arena.eval(expr, &chunk).expect("eval");
    let out = out.as_any().downcast_ref::<BinaryArray>().expect("binary");

    assert!(out.is_null(0));
}

// ---------------------------------------------------------------------------
// Tests migrated from object/percentile_functions.rs
// ---------------------------------------------------------------------------

#[test]
fn percentile_hash_encodes_value_and_empty_state() {
    use novarocks::common::percentile;

    let mut arena = ExprArena::default();
    let arg = arena.push_typed(ExprNode::SlotId(SlotId(1)), DataType::Int32);
    let input = Arc::new(Int32Array::from(vec![Some(7), None])) as ArrayRef;
    let chunk = one_col_chunk(DataType::Int32, input);

    let out = eval_percentile_hash(&arena, arg, &[arg], &chunk).expect("eval");
    let out = out.as_any().downcast_ref::<BinaryArray>().expect("binary");

    let decoded = percentile::decode_state(out.value(0)).expect("decode");
    assert_eq!(decoded.digest.count(), 1.0);
    assert_eq!(percentile::quantile_value(&decoded, 0.5), Some(7.0));
    let empty = percentile::decode_state(out.value(1)).expect("decode empty");
    assert_eq!(empty.digest.count(), 0.0);
    assert!(empty.quantiles.is_none());
}
