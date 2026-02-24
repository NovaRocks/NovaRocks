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

#[cfg(test)]
mod tests {
    use super::eval_crc32;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, field_with_slot_id};
    use crate::exec::expr::{ExprArena, ExprNode};
    use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    #[test]
    fn crc32_utf8_values_match_zlib() {
        let mut arena = ExprArena::default();
        let arg = arena.push_typed(ExprNode::SlotId(SlotId(1)), DataType::Utf8);

        let input = Arc::new(StringArray::from(vec![Some("123"), Some("abc"), None])) as ArrayRef;
        let field = field_with_slot_id(Field::new("c1", DataType::Utf8, true), SlotId(1));
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema, vec![input]).expect("record batch");
        let chunk = Chunk::new(batch);

        let out = eval_crc32(&arena, arg, &[arg], &chunk).expect("eval crc32");
        let out = out
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 array");
        assert_eq!(out.value(0), 2_286_445_522);
        assert_eq!(out.value(1), 891_568_578);
        assert!(out.is_null(2));
    }
}
