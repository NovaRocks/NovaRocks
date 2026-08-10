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
use arrow::array::{Array, ArrayRef, BinaryArray, StringArray};
use arrow::compute::cast;
use novarocks_types::largeint;
use twox_hash::xxhash3_128::Hasher;

enum BytesArray {
    Utf8(StringArray),
    Binary(BinaryArray),
}

impl BytesArray {
    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Utf8(arr) => arr.is_null(row),
            Self::Binary(arr) => arr.is_null(row),
        }
    }

    fn bytes(&self, row: usize) -> &[u8] {
        match self {
            Self::Utf8(arr) => arr.value(row).as_bytes(),
            Self::Binary(arr) => arr.value(row),
        }
    }
}

fn to_bytes_array(array: ArrayRef, arg_idx: usize) -> Result<BytesArray, String> {
    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(BytesArray::Utf8(arr.clone()));
    }
    if let Some(arr) = array.as_any().downcast_ref::<BinaryArray>() {
        return Ok(BytesArray::Binary(arr.clone()));
    }
    // Iceberg-backed VARBINARY/VARCHAR columns arrive as the "Large" Arrow
    // layout; normalize to the small layout via cast, then re-enter (the
    // casted array is Binary/Utf8 so this does not recurse again).
    if matches!(
        array.data_type(),
        arrow::datatypes::DataType::LargeBinary | arrow::datatypes::DataType::LargeUtf8
    ) {
        let target = if matches!(array.data_type(), arrow::datatypes::DataType::LargeUtf8) {
            arrow::datatypes::DataType::Utf8
        } else {
            arrow::datatypes::DataType::Binary
        };
        if let Ok(casted) = cast(&array, &target) {
            return to_bytes_array(casted, arg_idx);
        }
    }
    Err(format!(
        "xx_hash3_128: arg{} must be VARCHAR or VARBINARY",
        arg_idx
    ))
}

pub fn eval_xx_hash3_128(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let mut inputs = Vec::with_capacity(args.len());
    for (idx, arg) in args.iter().enumerate() {
        inputs.push(to_bytes_array(arena.eval(*arg, chunk)?, idx)?);
    }

    let mut out = Vec::with_capacity(chunk.len());
    for row in 0..chunk.len() {
        if inputs.iter().any(|arr| arr.is_null(row)) {
            out.push(None);
            continue;
        }

        let mut hasher = Hasher::with_seed(0);
        for input in &inputs {
            hasher.write(input.bytes(row));
        }
        out.push(Some(hasher.finish_128() as i128));
    }

    let out = largeint::array_from_i128(&out)?;
    let Some(target) = arena.data_type(expr) else {
        return Ok(out);
    };
    if out.data_type() == target || largeint::is_largeint_data_type(target) {
        return Ok(out);
    }
    cast(&out, target).map_err(|e| format!("xx_hash3_128: failed to cast output: {}", e))
}
