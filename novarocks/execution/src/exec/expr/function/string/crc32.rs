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
use arrow::array::{
    Array, ArrayRef, BinaryArray, Int64Builder, LargeBinaryArray, LargeStringArray, StringArray,
};
use std::sync::Arc;

pub fn eval_crc32(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let input = arena.eval(args[0], chunk)?;
    let mut builder = Int64Builder::with_capacity(input.len());

    match input.data_type() {
        arrow::datatypes::DataType::Utf8 => {
            let typed = input
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "downcast StringArray failed".to_string())?;
            for row in 0..typed.len() {
                if typed.is_null(row) {
                    builder.append_null();
                    continue;
                }
                builder.append_value(crc32_zlib(typed.value(row).as_bytes()) as i64);
            }
        }
        arrow::datatypes::DataType::LargeUtf8 => {
            let typed = input
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| "downcast LargeStringArray failed".to_string())?;
            for row in 0..typed.len() {
                if typed.is_null(row) {
                    builder.append_null();
                    continue;
                }
                builder.append_value(crc32_zlib(typed.value(row).as_bytes()) as i64);
            }
        }
        arrow::datatypes::DataType::Binary => {
            let typed = input
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| "downcast BinaryArray failed".to_string())?;
            for row in 0..typed.len() {
                if typed.is_null(row) {
                    builder.append_null();
                    continue;
                }
                builder.append_value(crc32_zlib(typed.value(row)) as i64);
            }
        }
        arrow::datatypes::DataType::LargeBinary => {
            let typed = input
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| "downcast LargeBinaryArray failed".to_string())?;
            for row in 0..typed.len() {
                if typed.is_null(row) {
                    builder.append_null();
                    continue;
                }
                builder.append_value(crc32_zlib(typed.value(row)) as i64);
            }
        }
        other => {
            return Err(format!(
                "crc32 expects VARCHAR/BINARY input, got {:?}",
                other
            ));
        }
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn crc32_zlib(data: &[u8]) -> u32 {
    let mut crc = 0xffff_ffff_u32;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xedb8_8320;
            } else {
                crc >>= 1;
            }
        }
    }
    crc ^ 0xffff_ffff
}
