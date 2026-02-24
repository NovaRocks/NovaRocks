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
/// Integration tests for CloneExpr functionality.
///
/// Tests verify that CloneExpr properly creates independent column copies
/// and aligns with StarRocks BE's CloneExpr behavior.
use novarocks::common::ids::SlotId;
use novarocks::exec::chunk::{Chunk, field_with_slot_id};
use novarocks::exec::expr::{ExprArena, ExprNode, LiteralValue};

use arrow::array::{Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

#[test]
fn test_clone_expr_eval_basic() {
    let mut arena = ExprArena::default();

    // Create a literal expression
    let child_id = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(42)), DataType::Int64);

    // Create a Clone expression wrapping the literal
    let clone_id = arena.push_typed(ExprNode::Clone(child_id), DataType::Int64);

    // Create a test chunk
    let schema = Schema::new(vec![field_with_slot_id(
        Field::new("dummy", DataType::Int64, false),
        SlotId::new(1),
    )]);
    let array = Int64Array::from(vec![1, 2, 3]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();
    let chunk = Chunk::new(batch);

    // Evaluate the Clone expression
    let result = arena.eval(clone_id, &chunk);
    assert!(result.is_ok());

    let result_array = result.unwrap();
    assert_eq!(result_array.len(), 3);

    // Verify the result contains the cloned value
    let int_array = result_array.as_any().downcast_ref::<Int64Array>().unwrap();
    for i in 0..3 {
        assert_eq!(int_array.value(i), 42);
    }
}

#[test]
fn test_clone_expr_with_arithmetic() {
    let mut arena = ExprArena::default();

    // Create: 10 + 20
    let left = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(10)), DataType::Int64);
    let right = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(20)), DataType::Int64);
    let add_id = arena.push_typed(ExprNode::Add(left, right), DataType::Int64);

    // Create: CLONE(10 + 20)
    let clone_id = arena.push_typed(ExprNode::Clone(add_id), DataType::Int64);

    // Create test chunk
    let schema = Schema::new(vec![field_with_slot_id(
        Field::new("dummy", DataType::Int64, false),
        SlotId::new(1),
    )]);
    let array = Int64Array::from(vec![1]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();
    let chunk = Chunk::new(batch);

    // Evaluate
    let result = arena.eval(clone_id, &chunk).unwrap();
    let int_array = result.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(int_array.value(0), 30);
}

#[test]
fn test_clone_expr_independence() {
    // This test simulates the scenario where multiple expressions reference
    // the same column, and CloneExpr ensures they don't interfere with each other.

    let mut arena = ExprArena::default();

    // Create a base expression: literal 100
    let base_id = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(100)), DataType::Int64);

    // Create two independent references:
    // ref1 = CLONE(base)
    // ref2 = CLONE(base)
    let ref1_id = arena.push_typed(ExprNode::Clone(base_id), DataType::Int64);
    let ref2_id = arena.push_typed(ExprNode::Clone(base_id), DataType::Int64);

    // Create test chunk
    let schema = Schema::new(vec![field_with_slot_id(
        Field::new("dummy", DataType::Int64, false),
        SlotId::new(1),
    )]);
    let array = Int64Array::from(vec![1, 2, 3]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();
    let chunk = Chunk::new(batch);

    // Evaluate both references
    let result1 = arena.eval(ref1_id, &chunk).unwrap();
    let result2 = arena.eval(ref2_id, &chunk).unwrap();

    // Both should produce the same result
    let arr1 = result1.as_any().downcast_ref::<Int64Array>().unwrap();
    let arr2 = result2.as_any().downcast_ref::<Int64Array>().unwrap();

    for i in 0..3 {
        assert_eq!(arr1.value(i), 100);
        assert_eq!(arr2.value(i), 100);
    }
}

#[test]
fn test_clone_expr_with_null() {
    let mut arena = ExprArena::default();

    // Create NULL literal
    let null_id = arena.push_typed(ExprNode::Literal(LiteralValue::Null), DataType::Null);

    // Clone the NULL
    let clone_id = arena.push_typed(ExprNode::Clone(null_id), DataType::Null);

    // Create test chunk
    let schema = Schema::new(vec![field_with_slot_id(
        Field::new("dummy", DataType::Int64, false),
        SlotId::new(1),
    )]);
    let array = Int64Array::from(vec![1]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();
    let chunk = Chunk::new(batch);

    // Evaluate
    let result = arena.eval(clone_id, &chunk);
    assert!(result.is_ok());

    let result_array = result.unwrap();
    // NullArray has all elements as null
    assert_eq!(result_array.data_type(), &DataType::Null);
    assert_eq!(result_array.len(), 1);
}

#[test]
fn test_nested_clone_expr() {
    let mut arena = ExprArena::default();

    // Create: literal 50
    let base_id = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(50)), DataType::Int64);

    // Create: CLONE(base)
    let clone1_id = arena.push_typed(ExprNode::Clone(base_id), DataType::Int64);

    // Create: CLONE(CLONE(base))
    let clone2_id = arena.push_typed(ExprNode::Clone(clone1_id), DataType::Int64);

    // Create test chunk
    let schema = Schema::new(vec![field_with_slot_id(
        Field::new("dummy", DataType::Int64, false),
        SlotId::new(1),
    )]);
    let array = Int64Array::from(vec![1, 2]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();
    let chunk = Chunk::new(batch);

    // Evaluate nested clone
    let result = arena.eval(clone2_id, &chunk).unwrap();
    let int_array = result.as_any().downcast_ref::<Int64Array>().unwrap();

    for i in 0..2 {
        assert_eq!(int_array.value(i), 50);
    }
}
