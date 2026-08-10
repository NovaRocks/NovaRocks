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
use arrow::array::{Array, ArrayRef, Int32Array, StringArray};
use regex::Regex;
use std::sync::Arc;

pub fn eval_regexp_position(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let _ = expr;
    let len = chunk.len();
    let str_arr = arena.eval(args[0], chunk)?;
    let pat_arr = arena.eval(args[1], chunk)?;
    let s_arr = downcast_string_or_null(&str_arr, "regexp_position")?;
    let p_arr = downcast_string_or_null(&pat_arr, "regexp_position")?;

    let start_arr_ref = if args.len() >= 3 {
        Some(arena.eval(args[2], chunk)?)
    } else {
        None
    };
    let start_arr = start_arr_ref
        .as_ref()
        .filter(|a| !matches!(a.data_type(), arrow::datatypes::DataType::Null))
        .map(|arr| super::common::downcast_int_arg_array(arr, "regexp_position"))
        .transpose()?;

    let occurrence_arr_ref = if args.len() >= 4 {
        Some(arena.eval(args[3], chunk)?)
    } else {
        None
    };
    let occurrence_arr = occurrence_arr_ref
        .as_ref()
        .filter(|a| !matches!(a.data_type(), arrow::datatypes::DataType::Null))
        .map(|arr| super::common::downcast_int_arg_array(arr, "regexp_position"))
        .transpose()?;

    // A NULL literal arrives as a typed-Null array. If the user passed
    // start/occurrence but its array is Null-typed, the result is NULL on
    // every row; encode that as the array being `None` here.
    let start_is_null = args.len() >= 3 && start_arr.is_none();
    let occurrence_is_null = args.len() >= 4 && occurrence_arr.is_none();

    let mut out = Vec::with_capacity(len);
    for row in 0..len {
        let Some(s) = string_value_at(&s_arr, row) else {
            out.push(None);
            continue;
        };
        let Some(p) = string_value_at(&p_arr, row) else {
            out.push(None);
            continue;
        };
        if start_is_null || occurrence_is_null {
            out.push(None);
            continue;
        }
        if start_arr.as_ref().is_some_and(|arr| arr.is_null(row))
            || occurrence_arr.as_ref().is_some_and(|arr| arr.is_null(row))
        {
            out.push(None);
            continue;
        }

        let start_pos = start_arr.as_ref().map_or(1, |arr| arr.value(row));
        let occurrence = occurrence_arr.as_ref().map_or(1, |arr| arr.value(row));
        let pos = eval_row(s, p, start_pos, occurrence)?;
        out.push(Some(pos as i32));
    }

    Ok(Arc::new(Int32Array::from(out)) as ArrayRef)
}

fn downcast_string_or_null<'a>(
    arr: &'a ArrayRef,
    fname: &str,
) -> Result<Option<&'a StringArray>, String> {
    if matches!(arr.data_type(), arrow::datatypes::DataType::Null) {
        return Ok(None);
    }
    let s = arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| format!("{fname} expects string"))?;
    Ok(Some(s))
}

fn string_value_at<'a>(arr: &Option<&'a StringArray>, row: usize) -> Option<&'a str> {
    let arr = arr.as_ref()?;
    if arr.is_null(row) {
        None
    } else {
        Some(arr.value(row))
    }
}

fn eval_row(input: &str, pattern: &str, start_pos: i64, occurrence: i64) -> Result<i64, String> {
    if start_pos <= 0 || occurrence <= 0 {
        return Ok(-1);
    }
    let start_pos = start_pos as usize;
    let occurrence = occurrence as usize;
    let len_chars = input.chars().count();

    if pattern.is_empty() {
        let max_pos = len_chars + 1;
        if start_pos > max_pos {
            return Ok(-1);
        }
        let target = start_pos + occurrence - 1;
        return Ok(if target <= max_pos { target as i64 } else { -1 });
    }

    let start_byte = match char_pos_to_byte_offset(input, start_pos) {
        Some(offset) => offset,
        None => return Ok(-1),
    };

    let re = Regex::new(pattern)
        .map_err(|e| format!("Invalid regex expression: {pattern}. Detail message: {e}"))?;
    let suffix = &input[start_byte..];

    let mut seen = 0usize;
    for matched in re.find_iter(suffix) {
        seen += 1;
        if seen == occurrence {
            let byte_offset = start_byte + matched.start();
            return Ok((input[..byte_offset].chars().count() + 1) as i64);
        }
    }
    Ok(-1)
}

fn char_pos_to_byte_offset(input: &str, pos_1_based: usize) -> Option<usize> {
    if pos_1_based == 0 {
        return None;
    }
    if pos_1_based == 1 {
        return Some(0);
    }
    let target_zero_based = pos_1_based - 1;
    let len_chars = input.chars().count();
    if target_zero_based > len_chars {
        return None;
    }
    if target_zero_based == len_chars {
        return Some(input.len());
    }
    input
        .char_indices()
        .nth(target_zero_based)
        .map(|(idx, _)| idx)
}
