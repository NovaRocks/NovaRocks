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
use std::collections::HashMap;
use std::sync::Arc;

use super::common::OLAP_STRING_MAX_LENGTH;

const ASCII_UNINIT: i16 = -1;
const ASCII_DELETED: i16 = -2;

enum TranslateMap {
    Ascii(Box<[i16; 256]>),
    Utf8(HashMap<char, Option<char>>),
}

fn build_translate_map(from_str: &str, to_str: &str) -> TranslateMap {
    if from_str.is_ascii() && to_str.is_ascii() {
        let mut map = [ASCII_UNINIT; 256];
        let from_bytes = from_str.as_bytes();
        let to_bytes = to_str.as_bytes();

        let common = from_bytes.len().min(to_bytes.len());
        for idx in 0..common {
            let key = from_bytes[idx] as usize;
            if map[key] == ASCII_UNINIT {
                map[key] = to_bytes[idx] as i16;
            }
        }
        for b in from_bytes.iter().skip(common) {
            let key = *b as usize;
            if map[key] == ASCII_UNINIT {
                map[key] = ASCII_DELETED;
            }
        }
        return TranslateMap::Ascii(Box::new(map));
    }

    let mut map = HashMap::new();
    let from_chars: Vec<char> = from_str.chars().collect();
    let to_chars: Vec<char> = to_str.chars().collect();
    for (idx, ch) in from_chars.into_iter().enumerate() {
        if map.contains_key(&ch) {
            continue;
        }
        if idx < to_chars.len() {
            map.insert(ch, Some(to_chars[idx]));
        } else {
            map.insert(ch, None);
        }
    }
    TranslateMap::Utf8(map)
}

fn translate_ascii(src: &str, ascii_map: &[i16; 256]) -> String {
    let mut out = Vec::with_capacity(src.len());
    for b in src.as_bytes() {
        let mapped = ascii_map[*b as usize];
        if mapped == ASCII_UNINIT {
            out.push(*b);
        } else if mapped != ASCII_DELETED {
            out.push(mapped as u8);
        }
    }
    String::from_utf8_lossy(&out).to_string()
}

fn translate_utf8(src: &str, utf8_map: &HashMap<char, Option<char>>) -> Option<String> {
    let mut out = String::with_capacity(src.len());
    let mut out_bytes = 0usize;

    for ch in src.chars() {
        let next = match utf8_map.get(&ch) {
            Some(Some(mapped)) => Some(*mapped),
            Some(None) => None,
            None => Some(ch),
        };
        let Some(ch) = next else {
            continue;
        };

        out_bytes += ch.len_utf8();
        if out_bytes > OLAP_STRING_MAX_LENGTH {
            return None;
        }
        out.push(ch);
    }

    Some(out)
}

pub fn eval_translate(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let _ = expr;
    let src_arr = arena.eval(args[0], chunk)?;
    let from_arr = arena.eval(args[1], chunk)?;
    let to_arr = arena.eval(args[2], chunk)?;
    let len = chunk.len();

    // A NULL literal argument arrives as a NullArray; treat it as
    // always-null so the result is NULL for every row without erroring.
    let src_str = downcast_string_or_null(&src_arr)?;
    let from_str = downcast_string_or_null(&from_arr)?;
    let to_str = downcast_string_or_null(&to_arr)?;

    let mut out = Vec::with_capacity(len);
    for row in 0..len {
        let (Some(src), Some(from_s), Some(to_s)) = (
            value_at(&src_str, row),
            value_at(&from_str, row),
            value_at(&to_str, row),
        ) else {
            out.push(None);
            continue;
        };

        let translated = match build_translate_map(from_s, to_s) {
            TranslateMap::Ascii(map) => Some(translate_ascii(src, &map)),
            TranslateMap::Utf8(map) => translate_utf8(src, &map),
        };
        out.push(translated);
    }

    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}

fn downcast_string_or_null(arr: &ArrayRef) -> Result<Option<&StringArray>, String> {
    if matches!(arr.data_type(), arrow::datatypes::DataType::Null) {
        return Ok(None);
    }
    let s = arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "translate expects string".to_string())?;
    Ok(Some(s))
}

fn value_at<'a>(arr: &Option<&'a StringArray>, row: usize) -> Option<&'a str> {
    let arr = arr.as_ref()?;
    if arr.is_null(row) {
        None
    } else {
        Some(arr.value(row))
    }
}
