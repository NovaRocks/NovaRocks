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
use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::datatypes::DataType;
use std::sync::Arc;

// SUBSTRING function for Arrow arrays
// Supports both 2-arg (substr(str, pos)) and 3-arg (substr(str, pos, len)) forms
// Aligned with StarRocks BE implementation
pub fn eval_substring(
    arena: &ExprArena,
    str_expr: ExprId,
    start_expr: ExprId,
    length_expr: Option<ExprId>,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let str_array = arena.eval(str_expr, chunk)?;
    let start_array = arena.eval(start_expr, chunk)?;

    let len = str_array.len();

    // If length_expr is None, create a constant array with INT_MAX to indicate "to end of string"
    let length_array = if let Some(length_expr_id) = length_expr {
        arena.eval(length_expr_id, chunk)?
    } else {
        // No length provided, return from pos to end of string
        // Use a large constant value to indicate "to end"
        let max_int = i64::MAX;
        let length_values: Vec<Option<i64>> = (0..len).map(|_| Some(max_int)).collect();
        Arc::new(Int64Array::from_iter(length_values)) as ArrayRef
    };

    // Downcast arrays
    let str_arr = str_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "substring: first argument must be a string array".to_string())?;

    // Extract start values (convert to i64)
    let start_values: Vec<Option<i64>> = match start_array.data_type() {
        DataType::Int64 => {
            let arr = start_array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "failed to downcast start to Int64Array".to_string())?;
            (0..len)
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    }
                })
                .collect()
        }
        DataType::Int32 => {
            let arr = start_array
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .ok_or_else(|| "failed to downcast start to Int32Array".to_string())?;
            (0..len)
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i) as i64)
                    }
                })
                .collect()
        }
        DataType::Float64 => {
            let arr = start_array
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .ok_or_else(|| "failed to downcast start to Float64Array".to_string())?;
            (0..len)
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i) as i64)
                    }
                })
                .collect()
        }
        _ => return Err("substring: second argument must be a numeric array".to_string()),
    };

    // Extract length values (convert to i64)
    let length_values: Vec<Option<i64>> = match length_array.data_type() {
        DataType::Int64 => {
            let arr = length_array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "failed to downcast length to Int64Array".to_string())?;
            (0..len)
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    }
                })
                .collect()
        }
        DataType::Int32 => {
            let arr = length_array
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .ok_or_else(|| "failed to downcast length to Int32Array".to_string())?;
            (0..len)
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i) as i64)
                    }
                })
                .collect()
        }
        DataType::Float64 => {
            let arr = length_array
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .ok_or_else(|| "failed to downcast length to Float64Array".to_string())?;
            (0..len)
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i) as i64)
                    }
                })
                .collect()
        }
        _ => return Err("substring: third argument must be a numeric array".to_string()),
    };

    // Process each row
    // Aligned with StarRocks BE implementation:
    // - pos > 0: count from left (1-based), pos=1 means first character
    // - pos < 0: count from right, pos=-1 means last character
    // - pos = 0: return empty string
    // - len <= 0: return empty string
    // - len = INT_MAX or not provided: return from pos to end of string
    let result_values: Vec<Option<String>> = (0..len)
        .map(|i| {
            if str_arr.is_null(i) || start_values[i].is_none() || length_values[i].is_none() {
                return None;
            }

            let s = str_arr.value(i);
            let pos = start_values[i].unwrap();
            let len_val = length_values[i].unwrap();

            // Handle pos = 0 or len <= 0: return empty string
            if pos == 0 || len_val <= 0 {
                return Some(String::new());
            }

            // Calculate start position (0-based)
            // StarRocks behavior:
            // - pos > 0: count from left (1-based), pos=1 means first character
            // - pos < 0: count from right, pos=-1 means last character
            let start_0based = if pos > 0 {
                // Positive position: count from left (1-based to 0-based)
                let pos_usize = (pos - 1) as usize;
                // Count characters (not bytes) for UTF-8 compatibility
                let char_count = s.chars().count();
                if pos_usize >= char_count {
                    // Position beyond string length, return empty
                    return Some(String::new());
                }
                // Convert character index to byte position
                s.char_indices()
                    .nth(pos_usize)
                    .map(|(idx, _)| idx)
                    .unwrap_or(s.len())
            } else {
                // Negative position: count from right
                // pos = -1 means last character, -2 means second to last, etc.
                let abs_pos = (-pos) as usize;
                let char_count = s.chars().count();
                if abs_pos > char_count {
                    // Position beyond string length from right, return empty
                    return Some(String::new());
                }
                // Find the byte position by counting characters from right
                let chars_to_skip = char_count - abs_pos;
                s.char_indices()
                    .nth(chars_to_skip)
                    .map(|(idx, _)| idx)
                    .unwrap_or(s.len())
            };

            // Calculate end position
            let end_0based = if len_val == i64::MAX {
                // No length specified or INT_MAX: return to end of string
                s.len()
            } else {
                // Length specified: take len_val characters from start_0based
                let len_usize = len_val as usize;
                // Find the byte position after taking len_usize characters from start_0based
                // Start from start_0based byte position
                s[start_0based..]
                    .char_indices()
                    .nth(len_usize)
                    .map(|(offset, _)| start_0based + offset)
                    .unwrap_or(s.len())
            };

            // Extract substring
            if start_0based >= s.len() {
                Some(String::new())
            } else {
                let end = end_0based.min(s.len());
                Some(s[start_0based..end].to_string())
            }
        })
        .collect();

    let result_array = StringArray::from_iter(result_values);
    Ok(Arc::new(result_array))
}
