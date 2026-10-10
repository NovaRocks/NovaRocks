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
//! Unchanged original runtime dispatcher values, errors and source ordering.
use super::{ExprArena, ExprId, ExprNode, LiteralValue};
use crate::exec::chunk::{Chunk, ChunkSchema};
use arrow::array::{Array, ArrayRef, BinaryArray, Int32Array, LargeStringArray, NullArray, StringArray};
use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_types::SlotId;
use std::sync::Arc;
fn setup(input: ArrayRef) -> (ExprArena, ExprId, Chunk) {
    let slot = SlotId::new(1);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "original-json",
            input.data_type().clone(),
            true,
        )])),
        vec![input.clone()],
    )
    .unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[slot]).unwrap();
    let mut arena = ExprArena::default();
    let child = arena.push_typed(ExprNode::SlotId(slot), input.data_type().clone());
    (arena, child, Chunk::new_with_chunk_schema(batch, schema))
}
fn strings(v: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(v))
}
fn call(arena: &ExprArena, args: &[ExprId], chunk: &Chunk) -> Result<ArrayRef, String> {
    super::function::variant::eval_variant_function(
        "parse_json",
        arena,
        ExprId(usize::MAX),
        args,
        chunk,
    )
}
fn values(a: ArrayRef) -> Vec<Option<String>> {
    assert_eq!(a.data_type(), &arrow::datatypes::DataType::Utf8);
    a.as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|v| v.map(str::to_owned))
        .collect()
}
fn raw(a: ArrayRef) -> Result<ArrayRef, String> {
    let (arena, child, chunk) = setup(a);
    call(&arena, &[child], &chunk)
}
#[test]
fn original_parse_json_canonical_recursive_unicode_order_and_duplicate_keys() {
    let input = strings(vec![
        Some(r#" { "z": [true,null,"é中🙂"], "a": 2.5 } "#),
        Some(r#"{"b":1,"a":2,"b":3}"#),
        Some(r#"[1, {"b":false,"a":"x\n"}, []]"#),
        Some(r#""a\u0000""#),
        Some("null"),
        Some("true"),
        Some("42"),
    ]);
    assert_eq!(
        values(raw(input).unwrap()),
        vec![
            Some(r#"{"a": 2.5, "z": [true, null, "é中🙂"]}"#.into()),
            Some(r#"{"a": 2, "b": 3}"#.into()),
            Some(r#"[1, {"a": "x\n", "b": false}, []]"#.into()),
            Some(r#""a\u0000""#.into()),
            Some("null".into()),
            Some("true".into()),
            Some("42".into())
        ]
    );
}
#[test]
fn original_parse_json_invalid_success_null_and_hidden_root_null() {
    let invalid = strings(vec![
        Some(""),
        Some("not JSON"),
        Some("["),
        Some("{\"a\":}"),
        Some("01"),
        Some("NaN"),
        Some("1e10000"),
        Some(r#""\ud800""#),
        None,
    ]);
    assert_eq!(values(raw(invalid).unwrap()), vec![None; 9]);
    let hidden = "this hidden source must not be parsed";
    let payload = format!("{hidden}[1,2]");
    let a: ArrayRef = Arc::new(StringArray::new(
        OffsetBuffer::new(
            vec![
                0,
                i32::try_from(hidden.len()).unwrap(),
                i32::try_from(payload.len()).unwrap(),
            ]
            .into(),
        ),
        Buffer::from(payload.as_bytes()),
        Some(NullBuffer::from(vec![false, true])),
    ));
    assert_eq!(values(raw(a).unwrap()), vec![None, Some("[1, 2]".into())]);
    let deep = format!("{}0{}", "[".repeat(140), "]".repeat(140));
    assert_eq!(values(raw(strings(vec![Some(&deep)])).unwrap()), vec![None]);
}
#[test]
fn original_parse_json_slice_empty_and_null_carrier_downcast_before_mask() {
    let a = strings(vec![Some("guard"), Some("[1,2]"), None, Some("guard")]).slice(1, 2);
    assert_eq!(values(raw(a).unwrap()), vec![Some("[1, 2]".into()), None]);
    assert!(values(raw(strings(vec![])).unwrap()).is_empty());
    for rows in [0, 2] {
        for a in [
            Arc::new(NullArray::new(rows)) as ArrayRef,
            Arc::new(Int32Array::from(vec![None; rows])) as ArrayRef,
            Arc::new(LargeStringArray::from(vec![None::<&str>; rows])) as ArrayRef,
            Arc::new(BinaryArray::from(vec![None::<&[u8]>; rows])) as ArrayRef,
        ] {
            assert_eq!(raw(a).unwrap_err(), "parse_json expects VARCHAR input");
        }
    }
}
#[test]
fn original_parse_json_local_and_arena_arity_before_child_and_required_child_error() {
    let (mut arena, child, chunk) = setup(strings(vec![Some("[1,2]")]));
    let missing = ExprId(usize::MAX);
    for args in [vec![], vec![child, missing]] {
        assert_eq!(
            call(&arena, &args, &chunk).unwrap_err(),
            format!("parse_json expects 1 argument, got {}", args.len())
        );
    }
    assert_eq!(
        call(&arena, &[missing], &chunk).unwrap_err(),
        "invalid ExprId"
    );
    let kind = super::function::lookup_function("parse_json").unwrap();
    for args in [vec![], vec![child, missing]] {
        let arity = args.len();
        let root = arena.push_typed(
            ExprNode::FunctionCall { kind, args },
            arrow::datatypes::DataType::Utf8,
        );
        assert_eq!(
            arena.eval(root, &chunk).unwrap_err(),
            format!("parse_json expects 1 to 1 arguments, got {arity}")
        );
    }
}
#[test]
fn original_parse_json_literal_and_pool_broadcast_are_runtime_text_not_insert_variant_bytes() {
    let (mut arena, _, chunk) = setup(strings(vec![Some("unused"); 7]));
    let literal = arena.push_typed(
        ExprNode::Literal(LiteralValue::Utf8("[1,2]".into())),
        arrow::datatypes::DataType::Utf8,
    );
    let constant = super::pure_differential::constant(
        novarocks_type_contract::FunctionValueType::new(arrow::datatypes::DataType::Utf8, false),
        strings(vec![Some("[1,2]")]),
    );
    let pool = arena.push_typed(
        ExprNode::Constant(constant),
        arrow::datatypes::DataType::Utf8,
    );
    for child in [literal, pool] {
        assert_eq!(
            values(call(&arena, &[child], &chunk).unwrap()),
            vec![Some("[1, 2]".into()); 7]
        );
    }
}
