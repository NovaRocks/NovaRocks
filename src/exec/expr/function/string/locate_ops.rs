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
use std::sync::Arc;

fn eval_locate_impl(
    arena: &ExprArena,
    args: &[ExprId],
    chunk: &Chunk,
    needle_arg: usize,
    haystack_arg: usize,
    start_arg: Option<usize>,
    fn_name: &str,
) -> Result<ArrayRef, String> {
    let haystack_arr = arena.eval(args[haystack_arg], chunk)?;
    let needle_arr = arena.eval(args[needle_arg], chunk)?;
    let start_raw = match start_arg {
        Some(idx) => Some(arena.eval(args[idx], chunk)?),
        None => None,
    };
    let haystack_arr = haystack_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| format!("{fn_name} expects string"))?;
    let needle_arr = needle_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| format!("{fn_name} expects string"))?;
    let start_arr = start_raw
        .as_ref()
        .map(|arr| super::common::downcast_int_arg_array(arr, fn_name))
        .transpose()?;

    let mut rows = haystack_arr.len().max(needle_arr.len());
    if let Some(start) = &start_arr {
        rows = rows.max(start.len());
    }

    if haystack_arr.len() != 1 && haystack_arr.len() != rows {
        return Err(format!(
            "{fn_name} haystack length mismatch: {} vs {rows}",
            haystack_arr.len()
        ));
    }
    if needle_arr.len() != 1 && needle_arr.len() != rows {
        return Err(format!(
            "{fn_name} needle length mismatch: {} vs {rows}",
            needle_arr.len()
        ));
    }
    if let Some(start) = &start_arr {
        if start.len() != 1 && start.len() != rows {
            return Err(format!(
                "{fn_name} start position length mismatch: {} vs {rows}",
                start.len()
            ));
        }
    }

    let mut out = Vec::with_capacity(rows);
    for row in 0..rows {
        let haystack_row = if haystack_arr.len() == 1 { 0 } else { row };
        let needle_row = if needle_arr.len() == 1 { 0 } else { row };
        let start_row = start_arr
            .as_ref()
            .map(|arr| if arr.len() == 1 { 0 } else { row });
        let start_is_null = match (&start_arr, start_row) {
            (Some(arr), Some(idx)) => arr.is_null(idx),
            _ => false,
        };

        if haystack_arr.is_null(haystack_row) || needle_arr.is_null(needle_row) || start_is_null {
            out.push(None);
            continue;
        }

        let start_pos = match (&start_arr, start_row) {
            (Some(arr), Some(idx)) => arr.value(idx),
            _ => 1,
        };
        let haystack = haystack_arr.value(haystack_row);
        let needle = needle_arr.value(needle_row);
        out.push(Some(locate_utf8(haystack, needle, start_pos)));
    }
    Ok(Arc::new(Int64Array::from(out)) as ArrayRef)
}

fn locate_utf8(haystack: &str, needle: &str, start_pos: i64) -> i64 {
    if start_pos <= 0 {
        return 0;
    }
    let haystack_chars = haystack.chars().count();
    if needle.is_empty() {
        let max_start = std::cmp::max(haystack_chars, 1) as i64;
        return if start_pos <= max_start { start_pos } else { 0 };
    }
    if start_pos as usize > haystack_chars {
        return 0;
    }

    let start_char_idx = (start_pos - 1) as usize;
    let start_byte = if start_char_idx == 0 {
        0
    } else {
        match haystack.char_indices().nth(start_char_idx) {
            Some((idx, _)) => idx,
            None => return 0,
        }
    };

    let rel_byte = match haystack[start_byte..].find(needle) {
        Some(v) => v,
        None => return 0,
    };
    let abs_byte = start_byte + rel_byte;
    (haystack[..abs_byte].chars().count() as i64) + 1
}

pub fn eval_locate(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    match args.len() {
        2 => eval_locate_impl(arena, args, chunk, 0, 1, None, "locate"),
        3 => eval_locate_impl(arena, args, chunk, 0, 1, Some(2), "locate"),
        _ => Err(format!(
            "locate expects 2 or 3 arguments, got {}",
            args.len()
        )),
    }
}

pub fn eval_instr(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() != 2 {
        return Err(format!("instr expects 2 arguments, got {}", args.len()));
    }
    eval_locate_impl(arena, args, chunk, 1, 0, None, "instr")
}

fn resolve_row_index(len: usize, rows: usize, row: usize, arg_name: &str) -> Result<usize, String> {
    if len == 1 {
        Ok(0)
    } else if len == rows {
        Ok(row)
    } else {
        Err(format!("{arg_name} length mismatch: {len} vs {rows}"))
    }
}

fn strpos_impl(haystack: &str, needle: &str, instance: i64) -> i64 {
    if instance == 0 {
        return 0;
    }
    if needle.is_empty() {
        return 1;
    }

    if instance > 0 {
        let mut found = 0_i64;
        let mut from_index = 0_usize;
        while found < instance {
            let Some(pos_rel) = haystack[from_index..].find(needle) else {
                return 0;
            };
            let pos = from_index + pos_rel;
            found += 1;
            if found == instance {
                return (pos + 1) as i64;
            }
            // Match StarRocks: advance by 1 byte to allow overlapping matches.
            from_index = pos + 1;
            if from_index > haystack.len() {
                break;
            }
        }
        return 0;
    }

    let target_from_end = (-instance) as usize;
    let mut positions = Vec::new();
    let mut from_index = 0_usize;
    while from_index <= haystack.len() {
        let Some(pos_rel) = haystack[from_index..].find(needle) else {
            break;
        };
        let pos = from_index + pos_rel;
        positions.push(pos);
        from_index = pos + 1;
    }
    if target_from_end == 0 || target_from_end > positions.len() {
        return 0;
    }
    (positions[positions.len() - target_from_end] + 1) as i64
}

pub fn eval_strpos(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() != 2 && args.len() != 3 {
        return Err(format!(
            "strpos expects 2 or 3 arguments, got {}",
            args.len()
        ));
    }

    let haystack_arr = arena.eval(args[0], chunk)?;
    let needle_arr = arena.eval(args[1], chunk)?;
    let instance_raw = if args.len() == 3 {
        Some(arena.eval(args[2], chunk)?)
    } else {
        None
    };

    let haystack_arr = haystack_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "strpos expects string".to_string())?;
    let needle_arr = needle_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "strpos expects string".to_string())?;
    let instance_arr = instance_raw
        .as_ref()
        .map(|arr| super::common::downcast_int_arg_array(arr, "strpos"))
        .transpose()?;

    let mut rows = haystack_arr.len().max(needle_arr.len());
    if let Some(instance) = &instance_arr {
        rows = rows.max(instance.len());
    }

    let mut out = Vec::with_capacity(rows);
    for row in 0..rows {
        let haystack_idx = resolve_row_index(haystack_arr.len(), rows, row, "strpos haystack")?;
        let needle_idx = resolve_row_index(needle_arr.len(), rows, row, "strpos needle")?;
        let instance_idx = if let Some(instance) = &instance_arr {
            Some(resolve_row_index(
                instance.len(),
                rows,
                row,
                "strpos instance",
            )?)
        } else {
            None
        };

        let instance_is_null = match (&instance_arr, instance_idx) {
            (Some(arr), Some(idx)) => arr.is_null(idx),
            _ => false,
        };
        if haystack_arr.is_null(haystack_idx) || needle_arr.is_null(needle_idx) || instance_is_null
        {
            out.push(None);
            continue;
        }
        let haystack = haystack_arr.value(haystack_idx);
        let needle = needle_arr.value(needle_idx);
        let instance = match (&instance_arr, instance_idx) {
            (Some(arr), Some(idx)) => arr.value(idx),
            _ => 1,
        };
        out.push(Some(strpos_impl(haystack, needle, instance)));
    }
    Ok(Arc::new(Int64Array::from(out)) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::strpos_impl;
    use crate::exec::expr::function::string::test_utils::assert_string_function_logic;

    #[test]
    fn test_locate_logic() {
        assert_string_function_logic("locate");
    }

    #[test]
    fn test_instr_logic() {
        assert_string_function_logic("instr");
    }

    #[test]
    fn test_strpos_logic() {
        assert_eq!(strpos_impl("abc", "b", 1), 2);
        assert_eq!(strpos_impl("abcabc", "abc", 2), 4);
        assert_eq!(strpos_impl("abcabc", "abc", -1), 4);
        assert_eq!(strpos_impl("abcabc", "abc", -2), 1);
        assert_eq!(strpos_impl("abcabc", "abc", -3), 0);
        assert_eq!(strpos_impl("abc", "", 1), 1);
        assert_eq!(strpos_impl("abc", "abc", 0), 0);
    }
}
