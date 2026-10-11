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

use super::*;
use crate::exec::{
    chunk::{Chunk, ChunkSchema},
    expr::{ExprArena, ExprNode},
};
use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{Field, Schema},
    record_batch::RecordBatch,
};
use novarocks_types::SlotId;
use std::sync::Arc;

fn evaluate(ws: bool, arrays: Vec<ArrayRef>, rows: usize) -> Result<ArrayRef, String> {
    let arguments = arrays.len();
    let arrays = if arrays.is_empty() {
        vec![Arc::new(Int64Array::from(vec![0; rows])) as ArrayRef]
    } else {
        arrays
    };
    let schema = Arc::new(Schema::new(
        arrays
            .iter()
            .enumerate()
            .map(|(i, a)| Field::new(format!("v{i}"), a.data_type().clone(), true))
            .collect::<Vec<_>>(),
    ));
    let slots = (0..arrays.len())
        .map(|i| SlotId::new(i as u32 + 1))
        .collect::<Vec<_>>();
    let chunk = Chunk::new_with_chunk_schema(
        RecordBatch::try_new(schema.clone(), arrays.clone()).unwrap(),
        ChunkSchema::try_ref_from_schema_and_slot_ids(&schema, &slots).unwrap(),
    );
    let mut arena = ExprArena::default();
    let args = arrays
        .iter()
        .zip(&slots)
        .take(arguments)
        .map(|(a, s)| arena.push_typed(ExprNode::SlotId(*s), a.data_type().clone()))
        .collect::<Vec<_>>();
    let frozen = arena.into_immutable().unwrap();
    let arena = ExprArena::from_immutable(&frozen).unwrap();
    let expr = args
        .first()
        .copied()
        .unwrap_or(crate::exec::expr::ExprId(0));
    if ws {
        concat_ws::eval_concat_ws(&arena, expr, &args, &chunk)
    } else {
        concat::eval_concat(&arena, expr, &args, &chunk)
    }
}
fn strings(v: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(v))
}
fn values(a: &ArrayRef) -> Vec<Option<&str>> {
    assert_eq!(a.data_type(), &arrow::datatypes::DataType::Utf8);
    a.as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .collect()
}

#[test]
fn legacy_concat_contract_tests_fixed_null_unicode_empty_and_raw_arities() {
    let a = strings(vec![Some("a"), None, Some(""), Some("é\0"), Some("")]);
    let b = strings(vec![Some("b"), Some("x"), Some(""), Some("你好"), None]);
    let c = strings(vec![Some("c"), Some(""), None, Some("👩‍💻"), None]);
    let sep = strings(vec![Some(","), Some("-"), Some("|"), Some(""), None]);
    let joined = evaluate(false, vec![a.clone(), b.clone(), c.clone()], 5).unwrap();
    assert_eq!(
        values(&joined),
        vec![Some("abc"), None, None, Some("é\0你好👩‍💻"), None]
    );
    let separated = evaluate(true, vec![sep, a, b, c], 5).unwrap();
    assert_eq!(
        values(&separated),
        vec![
            Some("a,b,c"),
            Some("x-"),
            Some("|"),
            Some("é\0你好👩‍💻"),
            None
        ]
    );
    assert_eq!(
        values(&evaluate(false, vec![], 3).unwrap()),
        vec![Some(""); 3]
    );
    assert_eq!(
        values(&evaluate(true, vec![strings(vec![Some("x"), None, Some("")])], 3).unwrap()),
        vec![Some(""), None, Some("")]
    );
    assert!(std::panic::catch_unwind(|| evaluate(true, vec![], 1)).is_err());
}
#[test]
fn legacy_concat_contract_tests_exact_byte_cap_used_separator_and_many_args() {
    let exact = "a".repeat(1_048_576);
    let below = "a".repeat(1_048_575);
    let above = "a".repeat(1_048_577);
    let a = strings(vec![Some(&exact), Some(&below), Some(&above)]);
    let tail = strings(vec![Some(""), Some("x"), Some("")]);
    let joined = evaluate(false, vec![a, tail], 3).unwrap();
    assert_eq!(
        values(&joined),
        vec![
            Some(exact.as_str()),
            Some(format!("{below}x").as_str()),
            None
        ]
    );
    let huge_sep = strings(vec![Some(&above); 3]);
    let single = evaluate(
        true,
        vec![
            huge_sep,
            strings(vec![Some("solo"), None, Some("")]),
            strings(vec![None; 3]),
        ],
        3,
    )
    .unwrap();
    assert_eq!(values(&single), vec![Some("solo"), Some(""), Some("")]);
    let capped = evaluate(
        true,
        vec![
            strings(vec![Some("x"); 3]),
            strings(vec![Some(&below), Some(&exact), Some(&below)]),
            strings(vec![Some(""), Some(""), Some("x")]),
        ],
        3,
    )
    .unwrap();
    assert_eq!(
        values(&capped),
        vec![Some(format!("{below}x").as_str()), None, None]
    );
    let parts = vec![strings(vec![Some("a")]); 320];
    assert_eq!(
        values(&evaluate(false, parts, 1).unwrap()),
        vec![Some("a".repeat(320).as_str())]
    );
    let mut parts = vec![strings(vec![Some("a")]); 320];
    parts[0] = strings(vec![Some("-")]);
    let expected = std::iter::repeat_n("a", 319).collect::<Vec<_>>().join("-");
    assert_eq!(
        values(&evaluate(true, parts, 1).unwrap()),
        vec![Some(expected.as_str())]
    );
}
#[test]
fn legacy_concat_contract_tests_eager_exact_carrier_messages_are_not_null_masked() {
    let wrong = Arc::new(Int64Array::from(vec![1])) as ArrayRef;
    assert_eq!(
        evaluate(false, vec![strings(vec![None]), wrong.clone()], 1).unwrap_err(),
        "concat expects string"
    );
    assert_eq!(
        evaluate(true, vec![wrong.clone(), strings(vec![None])], 1).unwrap_err(),
        "concat_ws expects string separator"
    );
    assert_eq!(
        evaluate(
            true,
            vec![strings(vec![None]), strings(vec![None]), wrong],
            1
        )
        .unwrap_err(),
        "concat_ws expects string"
    );
}
