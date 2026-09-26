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

use crate::common;
use arrow::array::{ArrayRef, BooleanArray, StringArray};
use arrow::datatypes::DataType;
use novarocks_execution::exec::chunk::Chunk;
use novarocks_execution::exec::expr::function::matching::{
    eval_ilike, eval_like, eval_regexp, register,
};
use novarocks_execution::exec::expr::{ExprArena, ExprId};
use regex::Regex;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// like tests (testing like_match via public eval_like)
// ---------------------------------------------------------------------------

#[test]
fn test_like_match_wildcards() {
    // Test the like_match logic via eval_like
    let mut arena = ExprArena::default();
    let expr = common::typed_null(&mut arena, DataType::Boolean);
    let chunk = common::chunk_len_1();

    // "abc" LIKE "a_c" => true
    let s = common::literal_string(&mut arena, "abc");
    let p = common::literal_string(&mut arena, "a_c");
    let str_expr = s;
    let pat_expr = p;
    let out = eval_like(&arena, str_expr, pat_expr, &chunk).unwrap();
    let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(out.value(0));

    // "abcdef" LIKE "a%f" => true
    let s2 = common::literal_string(&mut arena, "abcdef");
    let p2 = common::literal_string(&mut arena, "a%f");
    let out2 = eval_like(&arena, s2, p2, &chunk).unwrap();
    let out2 = out2.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(out2.value(0));

    // "abc" LIKE "a_d" => false
    let s3 = common::literal_string(&mut arena, "abc");
    let p3 = common::literal_string(&mut arena, "a_d");
    let out3 = eval_like(&arena, s3, p3, &chunk).unwrap();
    let out3 = out3.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(!out3.value(0));

    // suppress unused warning
    let _ = expr;
}

#[test]
fn test_like_match_backslash_escapes_wildcards() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();

    // "a_a" LIKE r"a\_a" => true
    let s = common::literal_string(&mut arena, "a_a");
    let p = common::literal_string(&mut arena, r"a\_a");
    let out = eval_like(&arena, s, p, &chunk).unwrap();
    let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(out.value(0));

    // "a%a" LIKE r"a\%a" => true
    let s2 = common::literal_string(&mut arena, "a%a");
    let p2 = common::literal_string(&mut arena, r"a\%a");
    let out2 = eval_like(&arena, s2, p2, &chunk).unwrap();
    let out2 = out2.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(out2.value(0));

    // r"a\a" LIKE r"a\\a" => true
    let s3 = common::literal_string(&mut arena, r"a\a");
    let p3 = common::literal_string(&mut arena, r"a\\a");
    let out3 = eval_like(&arena, s3, p3, &chunk).unwrap();
    let out3 = out3.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(out3.value(0));

    // "aba" LIKE r"a\_a" => false (backslash escapes _)
    let s4 = common::literal_string(&mut arena, "aba");
    let p4 = common::literal_string(&mut arena, r"a\_a");
    let out4 = eval_like(&arena, s4, p4, &chunk).unwrap();
    let out4 = out4.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(!out4.value(0));

    // "axa" LIKE r"a\%a" => false (backslash escapes %)
    let s5 = common::literal_string(&mut arena, "axa");
    let p5 = common::literal_string(&mut arena, r"a\%a");
    let out5 = eval_like(&arena, s5, p5, &chunk).unwrap();
    let out5 = out5.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(!out5.value(0));
}

#[test]
fn test_like_match_trailing_backslash_is_literal() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();

    // r"abc\" LIKE r"abc\" => true
    let s = common::literal_string(&mut arena, r"abc\");
    let p = common::literal_string(&mut arena, r"abc\");
    let out = eval_like(&arena, s, p, &chunk).unwrap();
    let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(out.value(0));

    // "abc" LIKE r"abc\" => false (pattern expects a trailing backslash)
    let s2 = common::literal_string(&mut arena, "abc");
    let p2 = common::literal_string(&mut arena, r"abc\");
    let out2 = eval_like(&arena, s2, p2, &chunk).unwrap();
    let out2 = out2.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(!out2.value(0));
}

// ---------------------------------------------------------------------------
// ilike tests
// ---------------------------------------------------------------------------

#[test]
fn test_ilike_case_insensitive_match() {
    let mut arena = ExprArena::default();
    let expr = common::typed_null(&mut arena, DataType::Boolean);
    let a = common::literal_string(&mut arena, "AbCd");
    let p = common::literal_string(&mut arena, "%bc_");

    let out = eval_ilike(&arena, expr, &[a, p], &common::chunk_len_1()).unwrap();
    let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(out.value(0));
}

// ---------------------------------------------------------------------------
// regexp tests
// ---------------------------------------------------------------------------

#[test]
fn test_regexp_partial_match() {
    let mut arena = ExprArena::default();
    let expr = common::typed_null(&mut arena, DataType::Boolean);
    let a = common::literal_string(&mut arena, "abc123xyz");
    let p = common::literal_string(&mut arena, "\\d+");

    let out = eval_regexp(&arena, expr, &[a, p], &common::chunk_len_1()).unwrap();
    let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(out.value(0));
}

fn string_column(values: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(values)) as ArrayRef
}

/// The error `regexp` reports for a pattern the regex engine rejects.
fn invalid_pattern_error(pattern: &str) -> String {
    format!(
        "regexp: invalid pattern: {}",
        Regex::new(pattern).unwrap_err()
    )
}

/// Evaluate `regexp(input, pattern)` over `chunk` and collect its rows.
fn regexp_rows(
    arena: &ExprArena,
    input: ExprId,
    pattern: ExprId,
    chunk: &Chunk,
) -> Result<Vec<Option<bool>>, String> {
    let out = eval_regexp(arena, input, &[input, pattern], chunk)?;
    let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
    Ok(out.iter().collect())
}

#[test]
fn test_regexp_constant_pattern_applies_to_every_row() {
    let chunk = common::chunk_from_columns(vec![string_column(vec![
        Some("abc1"),
        Some("xyz"),
        None,
        Some("zzabcz"),
        Some(""),
    ])]);
    let mut arena = ExprArena::default();
    let input = common::slot_ref(&mut arena, 1, DataType::Utf8);
    let pattern = common::literal_string(&mut arena, "abc.*");

    assert_eq!(
        regexp_rows(&arena, input, pattern, &chunk).unwrap(),
        vec![Some(true), Some(false), None, Some(true), Some(false)]
    );
}

#[test]
fn test_regexp_per_row_patterns_use_each_rows_own_pattern() {
    let chunk = common::chunk_from_columns(vec![
        string_column(vec![
            Some("abc"),
            Some("abc"),
            Some("xyz"),
            Some("123"),
            Some("abc"),
            Some("zz"),
        ]),
        string_column(vec![
            Some("^a"),
            Some("^z"),
            Some("y"),
            Some("\\d+"),
            Some("^a"),
            Some("^z"),
        ]),
    ]);
    let mut arena = ExprArena::default();
    let input = common::slot_ref(&mut arena, 1, DataType::Utf8);
    let pattern = common::slot_ref(&mut arena, 2, DataType::Utf8);

    assert_eq!(
        regexp_rows(&arena, input, pattern, &chunk).unwrap(),
        vec![
            Some(true),
            Some(false),
            Some(true),
            Some(true),
            Some(true),
            Some(true)
        ]
    );
}

#[test]
fn test_regexp_many_distinct_per_row_patterns_match_row_by_row_evaluation() {
    // About 200 distinct patterns, each recurring, which is more than one
    // evaluation retains: both retained and recompiled patterns are exercised.
    // Every third row carries a pattern that cannot match its input.
    let inputs = (0..300).map(|i| (i % 100).to_string()).collect::<Vec<_>>();
    let patterns = (0..300)
        .map(|i| {
            if i % 3 == 0 {
                format!("^x{}$", i % 100)
            } else {
                format!("^{}$", i % 100)
            }
        })
        .collect::<Vec<_>>();
    let columns = vec![
        string_column(inputs.iter().map(|s| Some(s.as_str())).collect()),
        string_column(patterns.iter().map(|s| Some(s.as_str())).collect()),
    ];
    let mut arena = ExprArena::default();
    let input = common::slot_ref(&mut arena, 1, DataType::Utf8);
    let pattern = common::slot_ref(&mut arena, 2, DataType::Utf8);

    let multi_row = regexp_rows(
        &arena,
        input,
        pattern,
        &common::chunk_from_columns(columns.clone()),
    )
    .unwrap();
    let row_by_row = common::single_row_chunks(&columns)
        .iter()
        .flat_map(|chunk| regexp_rows(&arena, input, pattern, chunk).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(multi_row, row_by_row);
    assert_eq!(
        multi_row
            .iter()
            .filter(|matched| **matched == Some(true))
            .count(),
        200
    );
}

#[test]
fn test_regexp_null_input_or_pattern_yields_null() {
    let chunk = common::chunk_from_columns(vec![
        string_column(vec![None, Some("abc"), None, Some("abc")]),
        string_column(vec![Some("a"), None, None, Some("a")]),
    ]);
    let mut arena = ExprArena::default();
    let input = common::slot_ref(&mut arena, 1, DataType::Utf8);
    let pattern = common::slot_ref(&mut arena, 2, DataType::Utf8);

    assert_eq!(
        regexp_rows(&arena, input, pattern, &chunk).unwrap(),
        vec![None, None, None, Some(true)]
    );
}

#[test]
fn test_regexp_invalid_constant_pattern_errors() {
    let chunk = common::chunk_from_columns(vec![string_column(vec![Some("a"), Some("b")])]);
    let mut arena = ExprArena::default();
    let input = common::slot_ref(&mut arena, 1, DataType::Utf8);
    let pattern = common::literal_string(&mut arena, "(unclosed");

    assert_eq!(
        regexp_rows(&arena, input, pattern, &chunk).unwrap_err(),
        invalid_pattern_error("(unclosed")
    );
}

#[test]
fn test_regexp_invalid_pattern_on_null_rows_only_is_not_an_error() {
    let chunk = common::chunk_from_columns(vec![string_column(vec![None, None])]);
    let mut arena = ExprArena::default();
    let input = common::slot_ref(&mut arena, 1, DataType::Utf8);
    let pattern = common::literal_string(&mut arena, "(unclosed");

    assert_eq!(
        regexp_rows(&arena, input, pattern, &chunk).unwrap(),
        vec![None, None]
    );
}

#[test]
fn test_regexp_invalid_per_row_pattern_errors_at_first_row_that_needs_it() {
    // Row 1 carries an invalid pattern but a NULL input, so the first
    // pattern actually compiled and rejected is row 2's.
    let chunk = common::chunk_from_columns(vec![
        string_column(vec![Some("a"), None, Some("x"), Some("y")]),
        string_column(vec![Some("a"), Some("(bad"), Some("[bad"), Some("(bad")]),
    ]);
    let mut arena = ExprArena::default();
    let input = common::slot_ref(&mut arena, 1, DataType::Utf8);
    let pattern = common::slot_ref(&mut arena, 2, DataType::Utf8);

    assert_eq!(
        regexp_rows(&arena, input, pattern, &chunk).unwrap_err(),
        invalid_pattern_error("[bad")
    );
}

// ---------------------------------------------------------------------------
// dispatch tests
// ---------------------------------------------------------------------------

#[test]
fn test_register_matching_functions() {
    use novarocks_execution::exec::expr::function::FunctionKind;
    use std::collections::HashMap;

    let mut m = HashMap::new();
    register(&mut m);
    assert_eq!(m.get("ilike"), Some(&FunctionKind::Matching("ilike")));
    assert_eq!(m.get("rlike"), Some(&FunctionKind::Matching("regexp")));
}
