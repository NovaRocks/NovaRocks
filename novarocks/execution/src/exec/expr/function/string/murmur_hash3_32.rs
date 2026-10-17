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
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
#[cfg(test)]
use std::sync::Arc;

#[cfg(test)]
const MURMUR3_32_SEED: u32 = 104_729;

pub fn eval_murmur_hash3_32<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let mut inputs = Vec::with_capacity(args.len());
    for arg in args {
        let input = arena.eval(*arg, chunk)?;
        // Unsupported selected-owner profiles retain the existing v1 carrier
        // projection. Their bytes still enter the single shared Murmur3 core.
        let numeric_text = matches!(
            input.data_type(),
            DataType::Float32
                | DataType::Float64
                | DataType::Decimal128(..)
                | DataType::Decimal256(..)
                | DataType::FixedSizeBinary(16)
        );
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

    Ok(
        (novarocks_functions::builtin::string_extended::evaluate_legacy(
            novarocks_functions::builtin::string_extended::StringOperation::Murmur,
            &inputs,
            chunk.len(),
        ))?,
    )
}

#[cfg(test)]
use novarocks_functions::builtin::string_extended::murmur_hash3_32;

#[cfg(test)]
mod tests {
    use super::*;

    use crate::exec::chunk::ChunkSchema;
    use crate::exec::expr::ExprNode;
    use crate::exec::expr::function::FunctionKind;
    use arrow::array::{
        Array, Decimal128Array, Decimal256Array, Float32Array, Float64Array, Int8Array, Int16Array,
        Int32Array, Int64Array, StringArray,
    };
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow_buffer::i256;
    use novarocks_type_contract::DecimalOverflowPolicy;
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
        let text = arena.push_typed(
            ExprNode::Cast(source, DecimalOverflowPolicy::OutputNull),
            DataType::Utf8,
        );
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
        let prepared =
            ExprArena::from_immutable(&frozen).expect("legacy frozen expression fixture");
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
