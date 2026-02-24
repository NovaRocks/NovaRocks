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
use crate::common::largeint;
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, Date32Array, Decimal128Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use sha2::{Digest, Sha256};
use std::sync::Arc;

const MARKER_NULL: u8 = 0;
const MARKER_INT8: u8 = 1;
const MARKER_INT16: u8 = 2;
const MARKER_INT32: u8 = 3;
const MARKER_INT64: u8 = 4;
const MARKER_INT128: u8 = 5;
const MARKER_FLOAT: u8 = 6;
const MARKER_DOUBLE: u8 = 7;
const MARKER_STRING: u8 = 8;
const MARKER_DECIMAL: u8 = 9;
const MARKER_DATE: u8 = 10;
const MARKER_DATETIME: u8 = 11;

#[inline]
fn update_null(digests: &mut [Sha256], row: usize) {
    digests[row].update([MARKER_NULL]);
}

#[inline]
fn update_marker_and_bytes(digests: &mut [Sha256], row: usize, marker: u8, bytes: &[u8]) {
    digests[row].update([marker]);
    digests[row].update(bytes);
}

fn encode_string_like(
    arr: &ArrayRef,
    digests: &mut [Sha256],
    is_binary: bool,
) -> Result<(), String> {
    if is_binary {
        let arr = arr.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| {
            "encode_fingerprint_sha256: failed to downcast BinaryArray".to_string()
        })?;
        for row in 0..arr.len() {
            if arr.is_null(row) {
                update_null(digests, row);
            } else {
                update_marker_and_bytes(digests, row, MARKER_STRING, arr.value(row));
            }
        }
    } else {
        let arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
            "encode_fingerprint_sha256: failed to downcast StringArray".to_string()
        })?;
        for row in 0..arr.len() {
            if arr.is_null(row) {
                update_null(digests, row);
            } else {
                update_marker_and_bytes(digests, row, MARKER_STRING, arr.value(row).as_bytes());
            }
        }
    }
    Ok(())
}

fn encode_fixed_size_binary(arr: &FixedSizeBinaryArray, digests: &mut [Sha256], marker: u8) {
    for row in 0..arr.len() {
        if arr.is_null(row) {
            update_null(digests, row);
        } else {
            update_marker_and_bytes(digests, row, marker, arr.value(row));
        }
    }
}

fn encode_timestamp(arr: &ArrayRef, digests: &mut [Sha256]) -> Result<(), String> {
    let casted = cast(arr, &DataType::Int64).map_err(|e| {
        format!("encode_fingerprint_sha256: failed to cast TIMESTAMP to BIGINT: {e}")
    })?;
    let casted = casted
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            "encode_fingerprint_sha256: failed to downcast timestamp cast".to_string()
        })?;
    for row in 0..casted.len() {
        if casted.is_null(row) {
            update_null(digests, row);
        } else {
            update_marker_and_bytes(
                digests,
                row,
                MARKER_DATETIME,
                &casted.value(row).to_le_bytes(),
            );
        }
    }
    Ok(())
}

fn encode_array(arr: &ArrayRef, digests: &mut [Sha256]) -> Result<(), String> {
    match arr.data_type() {
        DataType::Null => {
            for row in 0..arr.len() {
                update_null(digests, row);
            }
        }
        DataType::Utf8 => {
            encode_string_like(arr, digests, false)?;
        }
        DataType::Binary => {
            encode_string_like(arr, digests, true)?;
        }
        DataType::Boolean => {
            let arr = arr.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                "encode_fingerprint_sha256: failed to downcast BooleanArray".to_string()
            })?;
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    update_null(digests, row);
                } else {
                    update_marker_and_bytes(digests, row, MARKER_INT8, &[arr.value(row) as u8]);
                }
            }
        }
        DataType::Int8 => {
            let arr = arr.as_any().downcast_ref::<Int8Array>().ok_or_else(|| {
                "encode_fingerprint_sha256: failed to downcast Int8Array".to_string()
            })?;
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    update_null(digests, row);
                } else {
                    update_marker_and_bytes(
                        digests,
                        row,
                        MARKER_INT8,
                        &arr.value(row).to_le_bytes(),
                    );
                }
            }
        }
        DataType::UInt8 => {
            let arr = arr.as_any().downcast_ref::<UInt8Array>().ok_or_else(|| {
                "encode_fingerprint_sha256: failed to downcast UInt8Array".to_string()
            })?;
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    update_null(digests, row);
                } else {
                    update_marker_and_bytes(
                        digests,
                        row,
                        MARKER_INT8,
                        &arr.value(row).to_le_bytes(),
                    );
                }
            }
        }
        DataType::Int16 => {
            let arr = arr.as_any().downcast_ref::<Int16Array>().ok_or_else(|| {
                "encode_fingerprint_sha256: failed to downcast Int16Array".to_string()
            })?;
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    update_null(digests, row);
                } else {
                    update_marker_and_bytes(
                        digests,
                        row,
                        MARKER_INT16,
                        &arr.value(row).to_le_bytes(),
                    );
                }
            }
        }
        DataType::UInt16 => {
            let arr = arr.as_any().downcast_ref::<UInt16Array>().ok_or_else(|| {
                "encode_fingerprint_sha256: failed to downcast UInt16Array".to_string()
            })?;
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    update_null(digests, row);
                } else {
                    update_marker_and_bytes(
                        digests,
                        row,
                        MARKER_INT16,
                        &arr.value(row).to_le_bytes(),
                    );
                }
            }
        }
        DataType::Int32 => {
            let arr = arr.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                "encode_fingerprint_sha256: failed to downcast Int32Array".to_string()
            })?;
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    update_null(digests, row);
                } else {
                    update_marker_and_bytes(
                        digests,
                        row,
                        MARKER_INT32,
                        &arr.value(row).to_le_bytes(),
                    );
                }
            }
        }
        DataType::UInt32 => {
            let arr = arr.as_any().downcast_ref::<UInt32Array>().ok_or_else(|| {
                "encode_fingerprint_sha256: failed to downcast UInt32Array".to_string()
            })?;
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    update_null(digests, row);
                } else {
                    update_marker_and_bytes(
                        digests,
                        row,
                        MARKER_INT32,
                        &arr.value(row).to_le_bytes(),
                    );
                }
            }
        }
        DataType::Date32 => {
            let arr = arr.as_any().downcast_ref::<Date32Array>().ok_or_else(|| {
                "encode_fingerprint_sha256: failed to downcast Date32Array".to_string()
            })?;
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    update_null(digests, row);
                } else {
                    update_marker_and_bytes(
                        digests,
                        row,
                        MARKER_DATE,
                        &arr.value(row).to_le_bytes(),
                    );
                }
            }
        }
        DataType::Int64 => {
            let arr = arr.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                "encode_fingerprint_sha256: failed to downcast Int64Array".to_string()
            })?;
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    update_null(digests, row);
                } else {
                    update_marker_and_bytes(
                        digests,
                        row,
                        MARKER_INT64,
                        &arr.value(row).to_le_bytes(),
                    );
                }
            }
        }
        DataType::UInt64 => {
            let arr = arr.as_any().downcast_ref::<UInt64Array>().ok_or_else(|| {
                "encode_fingerprint_sha256: failed to downcast UInt64Array".to_string()
            })?;
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    update_null(digests, row);
                } else {
                    update_marker_and_bytes(
                        digests,
                        row,
                        MARKER_INT64,
                        &arr.value(row).to_le_bytes(),
                    );
                }
            }
        }
        DataType::Float32 => {
            let arr = arr.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                "encode_fingerprint_sha256: failed to downcast Float32Array".to_string()
            })?;
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    update_null(digests, row);
                } else {
                    update_marker_and_bytes(
                        digests,
                        row,
                        MARKER_FLOAT,
                        &arr.value(row).to_le_bytes(),
                    );
                }
            }
        }
        DataType::Float64 => {
            let arr = arr.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                "encode_fingerprint_sha256: failed to downcast Float64Array".to_string()
            })?;
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    update_null(digests, row);
                } else {
                    update_marker_and_bytes(
                        digests,
                        row,
                        MARKER_DOUBLE,
                        &arr.value(row).to_le_bytes(),
                    );
                }
            }
        }
        DataType::Timestamp(_, _) => {
            encode_timestamp(arr, digests)?;
        }
        DataType::Decimal128(precision, _) => {
            let arr = arr
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| {
                    "encode_fingerprint_sha256: failed to downcast Decimal128Array".to_string()
                })?;
            let (marker, byte_width) = if *precision <= 9 {
                (MARKER_INT32, 4usize)
            } else if *precision <= 18 {
                (MARKER_INT64, 8usize)
            } else {
                (MARKER_DECIMAL, 16usize)
            };
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    update_null(digests, row);
                } else {
                    let bytes = arr.value(row).to_le_bytes();
                    update_marker_and_bytes(digests, row, marker, &bytes[..byte_width]);
                }
            }
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let arr = largeint::as_fixed_size_binary_array(arr, "encode_fingerprint_sha256")?;
            encode_fixed_size_binary(arr, digests, MARKER_INT128);
        }
        // Keep unsupported complex/object-like types ignored to match StarRocks
        // runtime behavior for ENCODE_FINGERPRINT_SHA256.
        _ => {}
    }

    Ok(())
}

pub fn eval_encode_fingerprint_sha256(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let mut digests: Vec<Sha256> = (0..chunk.len()).map(|_| Sha256::new()).collect();
    for arg in args {
        let arr = arena.eval(*arg, chunk)?;
        encode_array(&arr, &mut digests)?;
    }

    let mut builder = BinaryBuilder::new();
    for digest in digests {
        builder.append_value(digest.finalize().to_vec());
    }

    let out = Arc::new(builder.finish()) as ArrayRef;
    super::common::cast_output(out, arena.data_type(expr), "encode_fingerprint_sha256")
}

#[cfg(test)]
mod tests {
    use super::eval_encode_fingerprint_sha256;
    use crate::exec::expr::function::encryption::test_utils::{
        chunk_len_1, literal_string, typed_null,
    };
    use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};
    use arrow::array::BinaryArray;
    use arrow::datatypes::DataType;

    fn literal_i64(arena: &mut ExprArena, v: i64) -> ExprId {
        arena.push(ExprNode::Literal(LiteralValue::Int64(v)))
    }

    #[test]
    fn test_encode_fingerprint_sha256_known_vectors() {
        let mut arena = ExprArena::default();
        let expr = typed_null(&mut arena, DataType::Binary);
        let null = typed_null(&mut arena, DataType::Null);
        let empty = literal_string(&mut arena, "");
        let max_i64 = literal_i64(&mut arena, i64::MAX);

        let out = eval_encode_fingerprint_sha256(&arena, expr, &[null], &chunk_len_1()).unwrap();
        let out = out.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(
            hex::encode_upper(out.value(0)),
            "6E340B9CFFB37A989CA544E6BB780A2C78901D3FB33738768511A30617AFA01D"
        );

        let out = eval_encode_fingerprint_sha256(&arena, expr, &[empty, max_i64], &chunk_len_1())
            .unwrap();
        let out = out.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(
            hex::encode_upper(out.value(0)),
            "940BF2CF7F45CCAB48F07F36838E94FD936B1417283FAAC8BF4E8B3E02865E69"
        );
    }
}
