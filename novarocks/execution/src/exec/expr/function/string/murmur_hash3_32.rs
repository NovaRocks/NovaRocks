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
    Array, ArrayRef, BinaryArray, BooleanArray, Int8Array, Int16Array, Int32Array, Int32Builder,
    Int64Array, LargeBinaryArray, LargeStringArray, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, TimeUnit};
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
        let input = arena.eval(*arg, chunk)?;
        // The public function accepts typed numeric arguments and hashes their
        // VARCHAR representation. Use the existing CAST owner once per batch,
        // including its zero, integral-float and exponent normalization.
        let numeric_text = matches!(
            input.data_type(),
            DataType::Float32
                | DataType::Float64
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
        ) || matches!(input.data_type(), DataType::FixedSizeBinary(width)
            if *width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH);
        if numeric_text {
            inputs.push(
                crate::exec::expr::cast_with_special_rules(&input, &DataType::Utf8).map_err(
                    |error| {
                        format!("numeric VARCHAR conversion for murmur_hash3_32 failed: {error}")
                    },
                )?,
            );
        } else {
            inputs.push(input);
        }
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
                // StarRocks coerces non-VARCHAR inputs to their textual form
                // via `ColumnViewer<TYPE_VARCHAR>`; mirror that so callers like
                // `murmur_hash3_32(ifnull(int_col, 0))` hash the value's decimal
                // representation rather than failing.
                _ => {
                    if let Some(s) = try_stringify_scalar(input, row)? {
                        seed = murmur_hash3_32(s.as_bytes(), seed);
                    } else {
                        has_null = true;
                        break;
                    }
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

/// Best-effort StarRocks-compatible `ColumnViewer<TYPE_VARCHAR>` on an arbitrary
/// scalar input array. Returns `None` when the row is NULL. Returns an error
/// for aggregate/nested types that StarRocks itself doesn't hash directly.
fn try_stringify_scalar(input: &ArrayRef, row: usize) -> Result<Option<String>, String> {
    if input.is_null(row) {
        return Ok(None);
    }
    macro_rules! cast {
        ($t:ty) => {{
            let arr = input
                .as_any()
                .downcast_ref::<$t>()
                .ok_or_else(|| format!("downcast {} failed", stringify!($t)))?;
            Ok(Some(arr.value(row).to_string()))
        }};
    }
    match input.data_type() {
        DataType::Int8 => cast!(Int8Array),
        DataType::Int16 => cast!(Int16Array),
        DataType::Int32 => cast!(Int32Array),
        DataType::Int64 => cast!(Int64Array),
        DataType::UInt8 => cast!(UInt8Array),
        DataType::UInt16 => cast!(UInt16Array),
        DataType::UInt32 => cast!(UInt32Array),
        DataType::UInt64 => cast!(UInt64Array),
        DataType::Boolean => {
            let arr = input
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "downcast BooleanArray failed".to_string())?;
            // StarRocks casts BOOLEAN → VARCHAR as "1"/"0".
            Ok(Some(if arr.value(row) { "1" } else { "0" }.to_string()))
        }
        // Arrow's default cast for Timestamp produces ISO 8601 with a `T`
        // separator (e.g. `2024-01-01T12:34:56`), but StarRocks's
        // VARCHAR-viewer of DATETIME uses a space (`2024-01-01 12:34:56`).
        // Use NovaRocks's StarRocks-compatible formatter so
        // `murmur_hash3_32(datetime_col)` hashes the same bytes StarRocks
        // would, including inside `array_map` over `array<datetime>`.
        DataType::Timestamp(unit, tz) => {
            let value = match unit {
                TimeUnit::Second => input
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .ok_or_else(|| "downcast TimestampSecondArray failed".to_string())?
                    .value(row),
                TimeUnit::Millisecond => input
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| "downcast TimestampMillisecondArray failed".to_string())?
                    .value(row),
                TimeUnit::Microsecond => input
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| "downcast TimestampMicrosecondArray failed".to_string())?
                    .value(row),
                TimeUnit::Nanosecond => input
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| "downcast TimestampNanosecondArray failed".to_string())?
                    .value(row),
            };
            Ok(Some(crate::exec::expr::cast::format_timestamp_for_varchar(
                unit,
                value,
                tz.as_deref(),
            )))
        }
        _ => {
            // Numeric text was converted once per batch by the public CAST owner.
            // Retain the existing Arrow formatter for the remaining carriers.
            use arrow::compute::kernels::cast::{CastOptions, cast_with_options};
            use arrow::util::display::FormatOptions;
            let opts = CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            };
            let casted = cast_with_options(input.as_ref(), &DataType::Utf8, &opts)
                .map_err(|e| format!("cast to Utf8 failed for murmur_hash3_32: {e}"))?;
            let arr = casted
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "cast result is not StringArray".to_string())?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(arr.value(row).to_string()))
            }
        }
    }
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
    use super::*;

    use crate::exec::chunk::ChunkSchema;
    use crate::exec::expr::ExprNode;
    use crate::exec::expr::function::FunctionKind;
    use arrow::array::{Decimal128Array, Decimal256Array, Float32Array, Float64Array};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow_buffer::i256;
    use novarocks_types::{SlotId, largeint};

    fn assert_prepared_numeric_text(input: ArrayRef, expected_text: &[Option<&str>]) {
        let input_type = input.data_type().clone();
        let schema = Arc::new(Schema::new(vec![Field::new("v", input_type.clone(), true)]));
        let batch = RecordBatch::try_new(schema, vec![input]).unwrap();
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1)],
        )
        .unwrap();
        let chunk = Chunk::new_with_chunk_schema(batch, chunk_schema);
        let mut arena = ExprArena::default();
        let source = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), input_type);
        let text = arena.push_typed(ExprNode::Cast(source), DataType::Utf8);
        let direct = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::String("murmur_hash3_32"),
                args: vec![source],
            },
            DataType::Int32,
        );
        let explicit = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::String("murmur_hash3_32"),
                args: vec![text],
            },
            DataType::Int32,
        );
        let frozen = arena.into_immutable().unwrap();
        let prepared = ExprArena::from_immutable(&frozen);
        let actual_text = prepared.eval(text, &chunk).unwrap();
        let actual_text = actual_text.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(actual_text.iter().collect::<Vec<_>>(), expected_text);
        let direct = prepared.eval(direct, &chunk).unwrap();
        let explicit = prepared.eval(explicit, &chunk).unwrap();
        let direct = direct.as_any().downcast_ref::<Int32Array>().unwrap();
        let explicit = explicit.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(
            direct.iter().collect::<Vec<_>>(),
            explicit.iter().collect::<Vec<_>>()
        );
        for (row, expected) in expected_text.iter().enumerate() {
            match expected {
                Some(text) => assert_eq!(
                    direct.value(row),
                    murmur_hash3_32(text.as_bytes(), MURMUR3_32_SEED) as i32
                ),
                None => assert!(direct.is_null(row)),
            }
        }
    }

    #[test]
    fn prepared_float_hash_uses_the_public_varchar_contract() {
        assert_prepared_numeric_text(
            Arc::new(Float32Array::from(vec![
                Some(0.0),
                Some(-0.0),
                Some(7.0),
                Some(1.25),
                None,
            ])),
            &[Some("0"), Some("0"), Some("7"), Some("1.25"), None],
        );
        assert_prepared_numeric_text(
            Arc::new(Float64Array::from(vec![
                Some(0.0),
                Some(-0.0),
                Some(7.0),
                Some(1.25),
                Some(1.2345678901234568e29),
                Some(f64::NAN),
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY),
                None,
            ])),
            &[
                Some("0"),
                Some("0"),
                Some("7"),
                Some("1.25"),
                Some("1.2345678901234568e+29"),
                Some("nan"),
                Some("inf"),
                Some("-inf"),
                None,
            ],
        );
    }

    #[test]
    fn prepared_signed_minima_hash_as_decimal_text() {
        assert_prepared_numeric_text(
            Arc::new(Int8Array::from(vec![Some(i8::MIN), Some(0), None])),
            &[Some("-128"), Some("0"), None],
        );
        assert_prepared_numeric_text(
            Arc::new(Int16Array::from(vec![Some(i16::MIN), Some(0), None])),
            &[Some("-32768"), Some("0"), None],
        );
        assert_prepared_numeric_text(
            Arc::new(Int32Array::from(vec![Some(i32::MIN), Some(0), None])),
            &[Some("-2147483648"), Some("0"), None],
        );
        assert_prepared_numeric_text(
            Arc::new(Int64Array::from(vec![Some(i64::MIN), Some(0), None])),
            &[Some("-9223372036854775808"), Some("0"), None],
        );
        assert_prepared_numeric_text(
            largeint::array_from_i128(&[Some(i128::MIN), Some(0), None]).unwrap(),
            &[
                Some("-170141183460469231731687303715884105728"),
                Some("0"),
                None,
            ],
        );
    }

    #[test]
    fn prepared_decimal128_hash_retains_declared_scale() {
        let input = Arc::new(
            Decimal128Array::from(vec![Some(123), Some(-100), Some(0), None])
                .with_precision_and_scale(18, 2)
                .unwrap(),
        ) as ArrayRef;
        assert_prepared_numeric_text(input, &[Some("1.23"), Some("-1.00"), Some("0.00"), None]);
    }

    #[test]
    fn prepared_decimal256_hash_retains_all_literal_digits_and_scale() {
        let exact: i256 = "123456789012345678901234567890123456789".parse().unwrap();
        let input = Arc::new(
            Decimal256Array::from(vec![Some(exact), Some(i256::ZERO), None])
                .with_precision_and_scale(39, 9)
                .unwrap(),
        ) as ArrayRef;
        assert_prepared_numeric_text(
            input,
            &[
                Some("123456789012345678901234567890.123456789"),
                Some("0.000000000"),
                None,
            ],
        );
        assert_eq!(
            murmur_hash3_32(b"123456789012345678901234567890.123456789", MURMUR3_32_SEED) as i32,
            1683874639
        );
        assert_eq!(
            murmur_hash3_32(b"1.2345678901234568e+29", MURMUR3_32_SEED) as i32,
            936035800
        );
    }

    /// NovaRocks treats embedded NUL bytes as content, not C-string
    /// terminators — so an 8-byte all-zero string and an empty string hash
    /// to different values, and `'\0\0\0\0\0\0\0\0'` round-trips through
    /// `<=>` joins. The SQL test case `join_fixed_size_string` step 30
    /// (join on `c_str8 <=> c_str8` with `'\0'×8` rows in both sides)
    /// relies on this property. Pin the exact value so a future regression
    /// (e.g. silently calling `strlen` on the byte slice) is caught
    /// without needing the 60K-row SQL test fixture.
    #[test]
    fn null_bytes_are_content_not_terminator() {
        assert_eq!(murmur_hash3_32(b"", MURMUR3_32_SEED), 3329588566);
        assert_eq!(murmur_hash3_32(b"\0", MURMUR3_32_SEED), 500407381);
        assert_eq!(
            murmur_hash3_32(b"\0\0\0\0\0\0\0\0", MURMUR3_32_SEED),
            1754797035
        );
    }
}
