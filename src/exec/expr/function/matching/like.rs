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
use arrow::array::{Array, ArrayRef, BooleanArray, StringArray};
use std::sync::Arc;

// Export like_match for use in constant evaluation
pub fn like_match(text: &str, pattern: &str) -> bool {
    let text_chars: Vec<char> = text.chars().collect();
    let pattern_chars: Vec<char> = pattern.chars().collect();
    like_match_recursive(&text_chars, 0, &pattern_chars, 0)
}

// Simple LIKE pattern matching algorithm (handles % and _)
fn like_match_recursive(
    text: &[char],
    text_idx: usize,
    pattern: &[char],
    pattern_idx: usize,
) -> bool {
    // If we've consumed all pattern characters, check if we've consumed all text
    if pattern_idx >= pattern.len() {
        return text_idx >= text.len();
    }

    match pattern[pattern_idx] {
        '%' => {
            // % matches zero or more characters
            // Try matching zero characters first (skip %)
            if like_match_recursive(text, text_idx, pattern, pattern_idx + 1) {
                return true;
            }
            // Try matching one or more characters
            for i in text_idx..text.len() {
                if like_match_recursive(text, i + 1, pattern, pattern_idx + 1) {
                    return true;
                }
            }
            false
        }
        '_' => {
            // _ matches exactly one character
            if text_idx >= text.len() {
                return false;
            }
            like_match_recursive(text, text_idx + 1, pattern, pattern_idx + 1)
        }
        c => {
            // Literal character must match
            if text_idx >= text.len() {
                return false;
            }
            if text[text_idx] == c {
                like_match_recursive(text, text_idx + 1, pattern, pattern_idx + 1)
            } else {
                false
            }
        }
    }
}

// LIKE function for Arrow arrays
pub fn eval_like(
    arena: &ExprArena,
    str_expr: ExprId,
    pattern_expr: ExprId,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let str_array = arena.eval(str_expr, chunk)?;
    let pattern_array = arena.eval(pattern_expr, chunk)?;

    let len = str_array.len();

    // Downcast arrays
    let str_arr = str_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "like: first argument must be a string array".to_string())?;

    let pattern_arr = pattern_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "like: second argument must be a string array".to_string())?;

    // Process each row
    let result_values: Vec<Option<bool>> = (0..len)
        .map(|i| {
            if str_arr.is_null(i) || pattern_arr.is_null(i) {
                return None;
            }

            let s = str_arr.value(i);
            let pattern = pattern_arr.value(i);

            Some(like_match(s, pattern))
        })
        .collect();

    let result_array = BooleanArray::from_iter(result_values);
    Ok(Arc::new(result_array))
}
