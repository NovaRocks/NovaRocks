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
use arrow::array::{Array, ArrayRef, ListBuilder, StringArray, StringBuilder};
use std::sync::Arc;

/// Evaluate split function.
/// Semantics align with StarRocks:
/// - split(str, delim) returns ARRAY<VARCHAR>
/// - empty delimiter splits into UTF-8 characters
/// - empty string with non-empty delimiter yields [""]
/// - if any argument is NULL, result is NULL
pub fn eval_split(
    arena: &ExprArena,
    str_expr: ExprId,
    delim_expr: ExprId,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let str_array = arena.eval(str_expr, chunk)?;
    let delim_array = arena.eval(delim_expr, chunk)?;

    let str_arr = str_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "split: first argument must be a string array".to_string())?;
    let delim_arr = delim_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "split: second argument must be a string array".to_string())?;

    let len = str_arr.len();
    if delim_arr.len() != len {
        return Err("split: argument length mismatch".to_string());
    }

    let value_builder = StringBuilder::new();
    let mut list_builder = ListBuilder::new(value_builder);

    for row in 0..len {
        if str_arr.is_null(row) || delim_arr.is_null(row) {
            list_builder.append(false);
            continue;
        }

        let haystack = str_arr.value(row);
        let delimiter = delim_arr.value(row);

        if delimiter.is_empty() {
            for ch in haystack.chars() {
                let mut buf = [0u8; 4];
                let s = ch.encode_utf8(&mut buf);
                list_builder.values().append_value(s);
            }
            list_builder.append(true);
            continue;
        }

        let mut start = 0usize;
        while let Some(pos) = haystack[start..].find(delimiter) {
            let end = start + pos;
            list_builder.values().append_value(&haystack[start..end]);
            start = end + delimiter.len();
        }
        list_builder.values().append_value(&haystack[start..]);
        list_builder.append(true);
    }

    Ok(Arc::new(list_builder.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::expr::function::string::test_utils::*;
    use arrow::array::{ListArray, StringArray};

    #[test]
    fn test_split_basic() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let s = literal_string(&mut arena, "a,b,c");
        let delim = literal_string(&mut arena, ",");

        let out = eval_split(&arena, s, delim, &chunk).unwrap();
        let list = out.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let start = list.offsets()[0] as usize;
        let end = list.offsets()[1] as usize;
        assert_eq!(end - start, 3);
        assert_eq!(values.value(start), "a");
        assert_eq!(values.value(start + 1), "b");
        assert_eq!(values.value(start + 2), "c");
    }
}
