// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::substring_index::eval_substring_index;
use crate::exec::{
    chunk::{Chunk, ChunkSchema},
    expr::{ExprArena, ExprId, ExprNode},
};
use arrow::{
    array::{ArrayRef, Float64Array, Int32Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use novarocks_types::SlotId;
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

fn fixture(arrays: Vec<ArrayRef>) -> (ExprArena, Vec<ExprId>, ExprId, Chunk) {
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
        .map(|(a, slot)| arena.push_typed(ExprNode::SlotId(*slot), a.data_type().clone()))
        .collect();
    // This raw result target is deliberately unrelated and never evaluated.
    let target = arena.push_typed(ExprNode::SlotId(SlotId::new(999)), DataType::Boolean);
    let frozen = arena.into_immutable().unwrap();
    (
        ExprArena::from_immutable(&frozen).unwrap(),
        args,
        target,
        chunk,
    )
}
fn evaluate(arrays: Vec<ArrayRef>) -> Result<ArrayRef, String> {
    let (arena, args, expr, chunk) = fixture(arrays);
    eval_substring_index(&arena, expr, &args, &chunk)
}
fn strings(values: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(values))
}
fn values(array: &ArrayRef) -> Vec<Option<&str>> {
    assert_eq!(array.data_type(), &DataType::Utf8);
    array
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .collect()
}

#[test]
fn legacy_substring_index_contract_tests_forward_spans_nulls_and_both_raw_count_widths() {
    let cases = [
        (Some("aaaaa"), Some("aa"), Some(1), Some("")),
        (Some("aaaaa"), Some("aa"), Some(-1), Some("a")),
        (Some("aaaaa"), Some("aa"), Some(-2), Some("aaa")),
        (Some("ababa"), Some("aba"), Some(-1), Some("ba")),
        (Some("/a//"), Some("/"), Some(-2), Some("/")),
        (Some("é::你好::\0"), Some("::"), Some(-2), Some("你好::\0")),
        (Some(""), Some("x"), Some(-1), Some("")),
        (Some("aaa"), Some("a"), Some(i32::MIN), Some("aaa")),
        (Some("aaa"), Some("a"), Some(i32::MAX), Some("aaa")),
        (Some("hidden"), Some(""), Some(-1), None),
        (Some("hidden"), Some("/"), Some(0), None),
        (None, Some("/"), Some(1), None),
        (Some("hidden"), None, Some(1), None),
        (Some("hidden"), Some("/"), None, None),
    ];
    let expected = cases.iter().map(|v| v.3).collect::<Vec<_>>();
    for wide in [false, true] {
        let counts = cases.iter().map(|v| v.2).collect::<Vec<_>>();
        let counts = if wide {
            Arc::new(Int64Array::from(
                counts
                    .into_iter()
                    .map(|v| v.map(i64::from))
                    .collect::<Vec<_>>(),
            )) as ArrayRef
        } else {
            Arc::new(Int32Array::from(counts)) as ArrayRef
        };
        let result = evaluate(vec![
            strings(cases.iter().map(|v| v.0).collect()),
            strings(cases.iter().map(|v| v.1).collect()),
            counts,
        ])
        .unwrap();
        assert_eq!(values(&result), expected);
    }
    let large = "x".repeat(1_048_577);
    let result = evaluate(vec![
        strings(vec![Some(&large)]),
        strings(vec![Some("y")]),
        Arc::new(Int64Array::from(vec![i64::MAX])),
    ])
    .unwrap();
    assert_eq!(values(&result), vec![Some(large.as_str())]);
}

#[test]
fn legacy_substring_index_contract_tests_int64_min_preserves_original_panic_reachability() {
    let matched = catch_unwind(AssertUnwindSafe(|| {
        evaluate(vec![
            strings(vec![Some("aaaaa")]),
            strings(vec![Some("aa")]),
            Arc::new(Int64Array::from(vec![i64::MIN])),
        ])
    }))
    .unwrap_err();
    let message = matched
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| matched.downcast_ref::<String>().map(String::as_str))
        .unwrap();
    assert_eq!(message, "attempt to negate with overflow");
    println!(
        "C12-P3 original matched raw Int64::MIN panic payload={message}; location is not an oracle"
    );
    for (text, delimiter, expected) in [
        (Some("no match"), Some("/"), Some("no match")),
        (Some("hidden"), Some(""), None),
        (None, Some("a"), None),
        (Some("a"), None, None),
    ] {
        let result = evaluate(vec![
            strings(vec![text]),
            strings(vec![delimiter]),
            Arc::new(Int64Array::from(vec![i64::MIN])),
        ])
        .unwrap();
        assert_eq!(values(&result), vec![expected]);
    }
}

#[test]
fn legacy_substring_index_contract_tests_carrier_messages_eager_children_and_ignored_raw_targets() {
    let integer = Arc::new(Int32Array::from(vec![1])) as ArrayRef;
    for bad in 0..2 {
        let mut arrays = vec![strings(vec![None]), strings(vec![None]), integer.clone()];
        arrays[bad] = integer.clone();
        assert_eq!(
            evaluate(arrays).unwrap_err(),
            "substring_index expects string"
        );
    }
    assert_eq!(
        evaluate(vec![
            strings(vec![None]),
            strings(vec![None]),
            Arc::new(Float64Array::from(vec![1.0]))
        ])
        .unwrap_err(),
        "substring_index expects int"
    );
    let (arena, good, expr, chunk) =
        fixture(vec![strings(vec![None]), strings(vec![Some("/")]), integer]);
    let earliest = arena.eval(expr, &chunk).unwrap_err();
    for args in [
        vec![expr, good[1], good[2]],
        vec![good[0], expr, good[2]],
        vec![good[0], good[1], expr],
        vec![good[0], expr, expr],
    ] {
        assert_eq!(
            eval_substring_index(&arena, expr, &args, &chunk).unwrap_err(),
            earliest
        );
    }
    let (arena, mut args, expr, chunk) = fixture(vec![
        strings(vec![Some("left/right")]),
        strings(vec![Some("/")]),
        Arc::new(Int32Array::from(vec![1])),
    ]);
    args.push(expr);
    let result = eval_substring_index(&arena, expr, &args, &chunk).unwrap();
    assert_eq!(values(&result), vec![Some("left")]);
    assert!(
        catch_unwind(AssertUnwindSafe(|| eval_substring_index(
            &arena,
            expr,
            &args[..2],
            &chunk
        )))
        .is_err()
    );
}
