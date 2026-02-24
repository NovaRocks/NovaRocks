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
use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryBuilder, Int8Array, Int16Array, Int32Array, Int64Array, ListArray,
    StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};

fn row_index(row: usize, len: usize) -> usize {
    if len == 1 { 0 } else { row }
}

fn parse_u64(text: &str) -> Option<u64> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    trimmed.parse::<u64>().ok()
}

fn encode_bitmap_text(values: &BTreeSet<u64>) -> Vec<u8> {
    values
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(",")
        .into_bytes()
}

fn collect_set(values: &ArrayRef, start: usize, end: usize) -> Result<BTreeSet<u64>, String> {
    let mut out = BTreeSet::new();
    match values.data_type() {
        DataType::Int8 => {
            let arr = values
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "failed to downcast Int8Array".to_string())?;
            for i in start..end {
                if arr.is_null(i) {
                    continue;
                }
                let v = arr.value(i);
                if v >= 0 {
                    out.insert(v as u64);
                }
            }
        }
        DataType::Int16 => {
            let arr = values
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "failed to downcast Int16Array".to_string())?;
            for i in start..end {
                if arr.is_null(i) {
                    continue;
                }
                let v = arr.value(i);
                if v >= 0 {
                    out.insert(v as u64);
                }
            }
        }
        DataType::Int32 => {
            let arr = values
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "failed to downcast Int32Array".to_string())?;
            for i in start..end {
                if arr.is_null(i) {
                    continue;
                }
                let v = arr.value(i);
                if v >= 0 {
                    out.insert(v as u64);
                }
            }
        }
        DataType::Int64 => {
            let arr = values
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "failed to downcast Int64Array".to_string())?;
            for i in start..end {
                if arr.is_null(i) {
                    continue;
                }
                let v = arr.value(i);
                if v >= 0 {
                    out.insert(v as u64);
                }
            }
        }
        DataType::UInt8 => {
            let arr = values
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or_else(|| "failed to downcast UInt8Array".to_string())?;
            for i in start..end {
                if arr.is_null(i) {
                    continue;
                }
                out.insert(arr.value(i) as u64);
            }
        }
        DataType::UInt16 => {
            let arr = values
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or_else(|| "failed to downcast UInt16Array".to_string())?;
            for i in start..end {
                if arr.is_null(i) {
                    continue;
                }
                out.insert(arr.value(i) as u64);
            }
        }
        DataType::UInt32 => {
            let arr = values
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| "failed to downcast UInt32Array".to_string())?;
            for i in start..end {
                if arr.is_null(i) {
                    continue;
                }
                out.insert(arr.value(i) as u64);
            }
        }
        DataType::UInt64 => {
            let arr = values
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| "failed to downcast UInt64Array".to_string())?;
            for i in start..end {
                if arr.is_null(i) {
                    continue;
                }
                out.insert(arr.value(i));
            }
        }
        DataType::Utf8 => {
            let arr = values
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "failed to downcast StringArray".to_string())?;
            for i in start..end {
                if arr.is_null(i) {
                    continue;
                }
                if let Some(v) = parse_u64(arr.value(i)) {
                    out.insert(v);
                }
            }
        }
        other => {
            return Err(format!(
                "array_to_bitmap expects integer/string array elements, got {:?}",
                other
            ));
        }
    }
    Ok(out)
}

pub fn eval_array_to_bitmap(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let input = arena.eval(args[0], chunk)?;
    let list = input.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        format!(
            "array_to_bitmap expects ARRAY input, got {:?}",
            input.data_type()
        )
    })?;
    let values = list.values();
    let offsets = list.value_offsets();

    let mut builder = BinaryBuilder::new();
    for row in 0..chunk.len() {
        let idx = row_index(row, list.len());
        if list.is_null(idx) {
            builder.append_null();
            continue;
        }
        let start = offsets[idx] as usize;
        let end = offsets[idx + 1] as usize;
        let set = collect_set(&values, start, end)?;
        builder.append_value(encode_bitmap_text(&set));
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}
