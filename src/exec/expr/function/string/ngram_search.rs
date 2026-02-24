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
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, StringArray};

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};

use super::common::downcast_int_arg_array;

const MAX_STRING_SIZE: usize = 1 << 15;

fn normalize_ascii_case(input: &str, case_insensitive: bool) -> Vec<u8> {
    if case_insensitive {
        input.bytes().map(|b| b.to_ascii_lowercase()).collect()
    } else {
        input.as_bytes().to_vec()
    }
}

fn build_gram_count_map(bytes: &[u8], gram_num: usize) -> HashMap<Vec<u8>, u16> {
    let mut map = HashMap::new();
    for gram in bytes.windows(gram_num) {
        let entry = map.entry(gram.to_vec()).or_insert(0);
        if *entry < u16::MAX {
            *entry += 1;
        }
    }
    map
}

fn ngram_similarity(
    haystack: &[u8],
    needle_gram_num: usize,
    gram_num: usize,
    needle_map: &HashMap<Vec<u8>, u16>,
) -> f64 {
    if haystack.len() > MAX_STRING_SIZE {
        return 0.0;
    }
    if haystack.len() < gram_num {
        return 0.0;
    }

    let mut remaining = needle_map.clone();
    let mut overlap = 0usize;
    for gram in haystack.windows(gram_num) {
        if let Some(freq) = remaining.get_mut(gram)
            && *freq > 0
        {
            *freq -= 1;
            overlap = overlap.saturating_add(1);
        }
    }
    let needle_not_overlap = needle_gram_num.saturating_sub(overlap);
    let score_f32 = 1.0_f32 - needle_not_overlap as f32 / needle_gram_num.max(1) as f32;
    score_f32 as f64
}

fn extract_const_string<'a>(
    arr: &'a StringArray,
    rows: usize,
    err_msg: &str,
) -> Result<Option<&'a str>, String> {
    if arr.len() == 1 {
        return if arr.is_null(0) {
            Ok(None)
        } else {
            Ok(Some(arr.value(0)))
        };
    }
    if arr.len() != rows {
        return Err(err_msg.to_string());
    }
    if arr.is_empty() {
        return Ok(None);
    }

    let first_is_null = arr.is_null(0);
    if first_is_null {
        if (1..arr.len()).all(|idx| arr.is_null(idx)) {
            return Ok(None);
        }
        return Err(err_msg.to_string());
    }
    let first = arr.value(0);
    if (1..arr.len()).all(|idx| !arr.is_null(idx) && arr.value(idx) == first) {
        Ok(Some(first))
    } else {
        Err(err_msg.to_string())
    }
}

fn extract_const_i64(
    arr: &super::common::IntArgArray<'_>,
    rows: usize,
    err_msg: &str,
) -> Result<Option<i64>, String> {
    if arr.len() == 1 {
        return if arr.is_null(0) {
            Ok(None)
        } else {
            Ok(Some(arr.value(0)))
        };
    }
    if arr.len() != rows {
        return Err(err_msg.to_string());
    }
    if arr.len() == 0 {
        return Ok(None);
    }

    let first_is_null = arr.is_null(0);
    if first_is_null {
        if (1..arr.len()).all(|idx| arr.is_null(idx)) {
            return Ok(None);
        }
        return Err(err_msg.to_string());
    }
    let first = arr.value(0);
    if (1..arr.len()).all(|idx| !arr.is_null(idx) && arr.value(idx) == first) {
        Ok(Some(first))
    } else {
        Err(err_msg.to_string())
    }
}

fn eval_ngram_search_impl(
    arena: &ExprArena,
    args: &[ExprId],
    chunk: &Chunk,
    case_insensitive: bool,
    fn_name: &str,
) -> Result<ArrayRef, String> {
    let haystack_arr = arena.eval(args[0], chunk)?;
    let needle_arr = arena.eval(args[1], chunk)?;
    let gram_num_arr = arena.eval(args[2], chunk)?;

    let haystack_arr = haystack_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| format!("{fn_name} expects string"))?;
    let needle_arr = needle_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| format!("{fn_name} expects string"))?;
    let gram_num_arr = downcast_int_arg_array(&gram_num_arr, fn_name)?;

    let rows = haystack_arr.len().max(chunk.len());
    if rows == 0 {
        return Ok(Arc::new(Float64Array::from(Vec::<Option<f64>>::new())) as ArrayRef);
    }
    if haystack_arr.len() != 1 && haystack_arr.len() != rows {
        return Err(format!(
            "{fn_name} haystack length mismatch: {} vs {rows}",
            haystack_arr.len()
        ));
    }
    let needle = extract_const_string(
        needle_arr,
        rows,
        "ngram search's second parameter must be const",
    )?;
    let gram_num_i64 = extract_const_i64(
        &gram_num_arr,
        rows,
        "ngram search's third parameter must be const",
    )?;

    let (Some(needle), Some(gram_num_i64)) = (needle, gram_num_i64) else {
        return Ok(Arc::new(Float64Array::from(vec![None; rows])) as ArrayRef);
    };
    if gram_num_i64 <= 0 {
        return Err("ngram search's third parameter must be a positive number".to_string());
    }
    let gram_num = usize::try_from(gram_num_i64)
        .map_err(|_| "ngram search's third parameter must be a positive number".to_string())?;

    if needle.len() > MAX_STRING_SIZE {
        return Err("ngram function's second parameter is larger than 2^15".to_string());
    }

    let normalized_needle = normalize_ascii_case(needle, case_insensitive);
    if normalized_needle.len() < gram_num {
        return Ok(Arc::new(Float64Array::from(vec![Some(0.0); rows])) as ArrayRef);
    }

    let needle_gram_num = normalized_needle.len() - gram_num + 1;
    let needle_map = build_gram_count_map(&normalized_needle, gram_num);

    let mut out = Vec::with_capacity(rows);
    for row in 0..rows {
        let haystack_idx = if haystack_arr.len() == 1 { 0 } else { row };
        if haystack_arr.is_null(haystack_idx) {
            out.push(None);
            continue;
        }
        let haystack = haystack_arr.value(haystack_idx);
        let normalized_haystack = normalize_ascii_case(haystack, case_insensitive);
        out.push(Some(ngram_similarity(
            &normalized_haystack,
            needle_gram_num,
            gram_num,
            &needle_map,
        )));
    }
    Ok(Arc::new(Float64Array::from(out)) as ArrayRef)
}

pub fn eval_ngram_search(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_ngram_search_impl(arena, args, chunk, false, "ngram_search")
}

pub fn eval_ngram_search_case_insensitive(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_ngram_search_impl(arena, args, chunk, true, "ngram_search_case_insensitive")
}

#[cfg(test)]
mod tests {
    use super::eval_ngram_search;
    use crate::exec::expr::ExprArena;
    use crate::exec::expr::function::string::dispatch::eval_string_function;
    use crate::exec::expr::function::string::test_utils::{
        chunk_len_1, literal_i64, literal_string, typed_null,
    };
    use arrow::array::Float64Array;
    use arrow::datatypes::DataType;

    #[test]
    fn test_ngram_search_basic() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr = typed_null(&mut arena, DataType::Float64);
        let haystack = literal_string(&mut arena, "chinese");
        let needle = literal_string(&mut arena, "china");
        let gram_num = literal_i64(&mut arena, 4);
        let out = eval_ngram_search(&arena, expr, &[haystack, needle, gram_num], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(out.value(0), 0.5);
    }

    #[test]
    fn test_ngram_search_case_insensitive_basic() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr = typed_null(&mut arena, DataType::Float64);
        let haystack = literal_string(&mut arena, "chinese");
        let needle = literal_string(&mut arena, "CHINESE");
        let gram_num = literal_i64(&mut arena, 4);
        let out = eval_string_function(
            "ngram_search_case_insensitive",
            &arena,
            expr,
            &[haystack, needle, gram_num],
            &chunk,
        )
        .unwrap();
        let out = out.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(out.value(0), 1.0);
    }
}
