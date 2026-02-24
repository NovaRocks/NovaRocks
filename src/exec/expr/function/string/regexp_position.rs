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
use regex::Regex;
use std::sync::Arc;

pub fn eval_regexp_position(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let _ = expr;
    let str_arr = arena.eval(args[0], chunk)?;
    let pat_arr = arena.eval(args[1], chunk)?;
    let s_arr = str_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "regexp_position expects string".to_string())?;
    let p_arr = pat_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "regexp_position expects string".to_string())?;

    let start_arr_ref = if args.len() >= 3 {
        Some(arena.eval(args[2], chunk)?)
    } else {
        None
    };
    let start_arr = start_arr_ref
        .as_ref()
        .map(|arr| super::common::downcast_int_arg_array(arr, "regexp_position"))
        .transpose()?;

    let occurrence_arr_ref = if args.len() >= 4 {
        Some(arena.eval(args[3], chunk)?)
    } else {
        None
    };
    let occurrence_arr = occurrence_arr_ref
        .as_ref()
        .map(|arr| super::common::downcast_int_arg_array(arr, "regexp_position"))
        .transpose()?;

    let mut out = Vec::with_capacity(s_arr.len());
    for row in 0..s_arr.len() {
        if s_arr.is_null(row) || p_arr.is_null(row) {
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
        let pos = eval_row(s_arr.value(row), p_arr.value(row), start_pos, occurrence)?;
        out.push(Some(pos));
    }

    Ok(Arc::new(Int64Array::from(out)) as ArrayRef)
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

#[cfg(test)]
mod tests {
    use super::eval_regexp_position;
    use crate::exec::expr::ExprArena;
    use crate::exec::expr::function::string::test_utils::assert_string_function_logic;
    use crate::exec::expr::function::string::test_utils::{
        chunk_len_1, literal_i64, literal_string, typed_null,
    };
    use arrow::array::Int64Array;
    use arrow::datatypes::DataType;

    #[test]
    fn test_regexp_position_logic() {
        assert_string_function_logic("regexp_position");
    }

    #[test]
    fn test_regexp_position_unicode_and_occurrence() {
        let mut arena = ExprArena::default();
        let expr = typed_null(&mut arena, DataType::Int64);
        let input = literal_string(&mut arena, "有朋$%X自9远方9来");
        let pat = literal_string(&mut arena, "[0-9]");
        let start = literal_i64(&mut arena, 10);
        let occ = literal_i64(&mut arena, 2);

        let out =
            eval_regexp_position(&arena, expr, &[input, pat, start, occ], &chunk_len_1()).unwrap();
        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out.value(0), -1);
    }
}
