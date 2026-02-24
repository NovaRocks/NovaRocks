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
use arrow::array::{Array, ArrayRef, StringArray};
use std::sync::Arc;

use super::common::downcast_int_arg_array;

pub fn eval_split_part(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let _ = expr;
    let str_arr = arena.eval(args[0], chunk)?;
    let delim_arr = arena.eval(args[1], chunk)?;
    let idx_arr = arena.eval(args[2], chunk)?;
    let s_arr = str_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "split_part expects string".to_string())?;
    let d_arr = delim_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "split_part expects string".to_string())?;
    let i_arr = downcast_int_arg_array(&idx_arr, "split_part")?;
    let len = s_arr.len();
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        if s_arr.is_null(i) || d_arr.is_null(i) || i_arr.is_null(i) {
            // Match StarRocks behavior for split_part: any NULL input yields empty string.
            out.push(Some(String::new()));
            continue;
        }
        let s = s_arr.value(i);
        let delim = d_arr.value(i);
        let idx = i_arr.value(i);
        out.push(Some(split_part_impl(s, delim, idx)));
    }
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}

fn split_part_impl(s: &str, delim: &str, idx: i64) -> String {
    if idx == 0 {
        return String::new();
    }

    if delim.is_empty() {
        if idx > s.len() as i64 {
            return String::new();
        }
        let mut h = 0usize;
        let mut num = 0i64;
        let target = idx.saturating_sub(1);
        while h < s.len() && num < target {
            let Some(ch) = s[h..].chars().next() else {
                return String::new();
            };
            h += ch.len_utf8();
            num += 1;
        }
        if h >= s.len() {
            return String::new();
        }
        return s[h..]
            .chars()
            .next()
            .map(|ch| ch.to_string())
            .unwrap_or_default();
    }

    if let Some(v) = split_index_impl(s, delim, idx) {
        v.to_string()
    } else if idx == 1 || idx == -1 {
        s.to_string()
    } else {
        String::new()
    }
}

fn split_index_impl<'a>(haystack: &'a str, delimiter: &str, part_number: i64) -> Option<&'a str> {
    if part_number > 0 {
        split_index_positive(haystack, delimiter, part_number)
    } else {
        split_index_negative(haystack, delimiter, part_number)
    }
}

fn split_index_positive<'a>(
    haystack: &'a str,
    delimiter: &str,
    part_number: i64,
) -> Option<&'a str> {
    let haystack_bytes = haystack.as_bytes();
    let delimiter_bytes = delimiter.as_bytes();
    let delimiter_len = delimiter_bytes.len() as isize;
    let mut pre_offset = -delimiter_len;
    let mut offset = -delimiter_len;
    let mut num = 0i64;

    while num < part_number {
        pre_offset = offset;
        let search_start = (offset + delimiter_len) as usize;
        if search_start > haystack_bytes.len() {
            break;
        }
        let search_slice = &haystack_bytes[search_start..];
        if let Some(pos_rel) = find_subslice(search_slice, delimiter_bytes) {
            offset = (search_start + pos_rel) as isize;
            num += 1;
        } else {
            offset = haystack_bytes.len() as isize;
            num = if num == 0 { 0 } else { num + 1 };
            break;
        }
    }

    if num == part_number {
        let start = (pre_offset + delimiter_len) as usize;
        let end = offset as usize;
        Some(&haystack[start..end])
    } else {
        None
    }
}

fn split_index_negative<'a>(
    haystack: &'a str,
    delimiter: &str,
    part_number: i64,
) -> Option<&'a str> {
    let target = usize::try_from(-part_number).ok()?;
    if target == 0 {
        return None;
    }

    let mut offset = haystack.len() as isize;
    let mut pre_offset = offset;
    let mut num = 0usize;
    let mut search_scope = haystack;

    while num <= target && offset >= 0 {
        if let Some(found) = search_scope.rfind(delimiter) {
            offset = found as isize;
            num += 1;
            if num == target {
                break;
            }
            pre_offset = offset;
            offset -= 1;
            search_scope = &haystack[..(pre_offset as usize)];
        } else {
            offset = -1;
            break;
        }
    }
    if offset == -1 && num != 0 {
        num += 1;
    }

    if num == target {
        if offset == -1 {
            Some(&haystack[..(pre_offset as usize)])
        } else {
            let start = offset as usize + delimiter.len();
            let end = pre_offset as usize;
            Some(&haystack[start..end])
        }
    } else {
        None
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    if haystack.len() < needle.len() {
        return None;
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}
#[cfg(test)]
mod tests {
    use super::split_part_impl;
    use crate::exec::expr::function::string::test_utils::assert_string_function_logic;

    #[test]
    fn test_split_part_logic() {
        assert_string_function_logic("split_part");
    }

    #[test]
    fn test_split_part_overlapped_delimiter_from_right() {
        assert_eq!(split_part_impl("abc##567###234", "##", -1), "234");
        assert_eq!(split_part_impl("abc##567###234", "##", -2), "567#");
    }
}
