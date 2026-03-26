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
use arrow::array::BooleanArray;
use arrow::datatypes::DataType;
use novarocks::exec::expr::ExprArena;
use novarocks::exec::expr::function::matching::{eval_ilike, eval_like, eval_regexp, register};

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

// ---------------------------------------------------------------------------
// dispatch tests
// ---------------------------------------------------------------------------

#[test]
fn test_register_matching_functions() {
    use novarocks::exec::expr::function::FunctionKind;
    use std::collections::HashMap;

    let mut m = HashMap::new();
    register(&mut m);
    assert_eq!(m.get("ilike"), Some(&FunctionKind::Matching("ilike")));
    assert_eq!(m.get("rlike"), Some(&FunctionKind::Matching("regexp")));
}
