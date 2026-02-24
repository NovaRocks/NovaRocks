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
    Array, ArrayRef, BinaryArray, Int32Builder, LargeBinaryArray, LargeStringArray, StringArray,
};
use arrow::datatypes::DataType;
use std::sync::Arc;

const MURMUR3_32_SEED: u32 = 104_729;

pub fn eval_murmur_hash3_32(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let mut inputs = Vec::with_capacity(args.len());
    for arg in args {
        inputs.push(arena.eval(*arg, chunk)?);
    }

    let mut builder = Int32Builder::with_capacity(chunk.len());
    for row in 0..chunk.len() {
        let mut seed = MURMUR3_32_SEED;
        let mut has_null = false;
        for input in &inputs {
            match input.data_type() {
                DataType::Utf8 => {
                    let arr = input
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| "downcast StringArray failed".to_string())?;
                    if arr.is_null(row) {
                        has_null = true;
                        break;
                    }
                    seed = murmur_hash3_32(arr.value(row).as_bytes(), seed);
                }
                DataType::LargeUtf8 => {
                    let arr = input
                        .as_any()
                        .downcast_ref::<LargeStringArray>()
                        .ok_or_else(|| "downcast LargeStringArray failed".to_string())?;
                    if arr.is_null(row) {
                        has_null = true;
                        break;
                    }
                    seed = murmur_hash3_32(arr.value(row).as_bytes(), seed);
                }
                DataType::Binary => {
                    let arr = input
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .ok_or_else(|| "downcast BinaryArray failed".to_string())?;
                    if arr.is_null(row) {
                        has_null = true;
                        break;
                    }
                    seed = murmur_hash3_32(arr.value(row), seed);
                }
                DataType::LargeBinary => {
                    let arr = input
                        .as_any()
                        .downcast_ref::<LargeBinaryArray>()
                        .ok_or_else(|| "downcast LargeBinaryArray failed".to_string())?;
                    if arr.is_null(row) {
                        has_null = true;
                        break;
                    }
                    seed = murmur_hash3_32(arr.value(row), seed);
                }
                other => {
                    return Err(format!(
                        "murmur_hash3_32 expects VARCHAR/VARBINARY input, got {:?}",
                        other
                    ));
                }
            }
        }
        if has_null {
            builder.append_null();
        } else {
            builder.append_value(seed as i32);
        }
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn murmur_hash3_32(data: &[u8], seed: u32) -> u32 {
    const C1: u32 = 0xcc9e2d51;
    const C2: u32 = 0x1b873593;

    let mut hash = seed;
    let mut chunks = data.chunks_exact(4);
    for chunk in &mut chunks {
        let mut k = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        k = k.wrapping_mul(C1);
        k = k.rotate_left(15);
        k = k.wrapping_mul(C2);
        hash ^= k;
        hash = hash.rotate_left(13);
        hash = hash.wrapping_mul(5).wrapping_add(0xe6546b64);
    }

    let rem = chunks.remainder();
    let mut k1 = 0u32;
    match rem.len() {
        3 => {
            k1 ^= (rem[2] as u32) << 16;
            k1 ^= (rem[1] as u32) << 8;
            k1 ^= rem[0] as u32;
        }
        2 => {
            k1 ^= (rem[1] as u32) << 8;
            k1 ^= rem[0] as u32;
        }
        1 => {
            k1 ^= rem[0] as u32;
        }
        _ => {}
    }
    if k1 != 0 {
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(15);
        k1 = k1.wrapping_mul(C2);
        hash ^= k1;
    }

    hash ^= data.len() as u32;
    hash ^= hash >> 16;
    hash = hash.wrapping_mul(0x85ebca6b);
    hash ^= hash >> 13;
    hash = hash.wrapping_mul(0xc2b2ae35);
    hash ^= hash >> 16;
    hash
}

#[cfg(test)]
mod tests {
    use super::eval_murmur_hash3_32;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, field_with_slot_id};
    use crate::exec::expr::{ExprArena, ExprNode};
    use arrow::array::{Array, ArrayRef, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    #[test]
    fn murmur_hash3_32_known_vectors() {
        let mut arena = ExprArena::default();
        let arg0 = arena.push_typed(ExprNode::SlotId(SlotId(1)), DataType::Utf8);
        let arg1 = arena.push_typed(ExprNode::SlotId(SlotId(2)), DataType::Utf8);

        let c1 = Arc::new(StringArray::from(vec![Some("test1234567"), Some("hello")])) as ArrayRef;
        let c2 = Arc::new(StringArray::from(vec![Some("asdf213"), Some("world")])) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("c1", DataType::Utf8, true), SlotId(1)),
            field_with_slot_id(Field::new("c2", DataType::Utf8, true), SlotId(2)),
        ]));
        let chunk = Chunk::new(RecordBatch::try_new(schema, vec![c1, c2]).expect("record batch"));

        let out_single = eval_murmur_hash3_32(&arena, arg0, &[arg0], &chunk).expect("eval single");
        let out_single = out_single
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 output");
        assert_eq!(out_single.value(0), -1_948_194_659);
        assert_eq!(out_single.value(1), 1_321_743_225);

        let out_multi =
            eval_murmur_hash3_32(&arena, arg0, &[arg0, arg1], &chunk).expect("eval multi");
        let out_multi = out_multi
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 output");
        assert_eq!(out_multi.value(0), -500_290_079);
        assert_eq!(out_multi.value(1), 984_713_481);
    }

    #[test]
    fn murmur_hash3_32_null_propagation() {
        let mut arena = ExprArena::default();
        let arg0 = arena.push_typed(ExprNode::SlotId(SlotId(1)), DataType::Utf8);
        let arg1 = arena.push_typed(ExprNode::SlotId(SlotId(2)), DataType::Utf8);

        let c1 = Arc::new(StringArray::from(vec![Some("hello"), Some("hello")])) as ArrayRef;
        let c2 = Arc::new(StringArray::from(vec![None, Some("world")])) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("c1", DataType::Utf8, true), SlotId(1)),
            field_with_slot_id(Field::new("c2", DataType::Utf8, true), SlotId(2)),
        ]));
        let chunk = Chunk::new(RecordBatch::try_new(schema, vec![c1, c2]).expect("record batch"));

        let out = eval_murmur_hash3_32(&arena, arg0, &[arg0, arg1], &chunk).expect("eval");
        let out = out
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 output");
        assert!(out.is_null(0));
        assert_eq!(out.value(1), 984_713_481);
    }
}
