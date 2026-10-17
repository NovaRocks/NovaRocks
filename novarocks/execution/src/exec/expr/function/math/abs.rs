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
    Array, ArrayRef, Decimal128Array, Decimal256Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array,
};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow_buffer::i256;
use novarocks_types::largeint;
use std::sync::Arc;

pub fn eval_abs<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    value_expr: ExprId,
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let value = arena.eval(value_expr, chunk)?;
    let output = arena
        .data_type(expr)
        .ok_or_else(|| "abs: missing output type".to_string())?;
    Ok(
        (novarocks_functions::builtin::abs_core::evaluate_abs_core(
            value,
            output,
            &mut |_| Ok(()),
        )
        .map_err(|error| error.to_string()))?,
    )
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::expr::ExprNode;
    use crate::exec::expr::function::FunctionKind;
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;

    fn prepared_abs(input: ArrayRef, output: DataType) -> Result<ArrayRef, String> {
        let input_type = input.data_type().clone();
        let schema = Arc::new(Schema::new(vec![Field::new("v", input_type.clone(), true)]));
        let batch = RecordBatch::try_new(schema, vec![input]).unwrap();
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1)],
        )
        .unwrap();
        let chunk = Chunk::new_with_chunk_schema(batch, chunk_schema);
        let mut decoder_arena = ExprArena::default();
        let source = decoder_arena.push_typed(ExprNode::SlotId(SlotId::new(1)), input_type.clone());
        let absolute = decoder_arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Abs,
                args: vec![source],
            },
            output.clone(),
        );
        let immutable = decoder_arena
            .into_immutable()
            .expect("freeze before runtime binding");
        assert_eq!(immutable.nodes()[source.0].data_type(), &input_type);
        assert_eq!(immutable.nodes()[absolute.0].data_type(), &output);
        let prepared =
            ExprArena::from_immutable(&immutable).expect("legacy frozen expression fixture");
        let result = prepared.eval(absolute, &chunk)?;
        assert_eq!(
            result.data_type(),
            &output,
            "one frozen output dtype applies to the whole batch"
        );
        Ok(result)
    }

    fn signed_values(array: &ArrayRef) -> Vec<Option<i128>> {
        (0..array.len())
            .map(|row| {
                if array.is_null(row) {
                    return None;
                }
                Some(match array.data_type() {
                    DataType::Int16 => i128::from(
                        array
                            .as_any()
                            .downcast_ref::<Int16Array>()
                            .unwrap()
                            .value(row),
                    ),
                    DataType::Int32 => i128::from(
                        array
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap()
                            .value(row),
                    ),
                    DataType::Int64 => i128::from(
                        array
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(row),
                    ),
                    data_type if largeint::is_largeint_data_type(data_type) => largeint::value_at(
                        largeint::as_fixed_size_binary_array(array, "test ABS").unwrap(),
                        row,
                    )
                    .unwrap(),
                    other => panic!("unexpected promoted integer output {other:?}"),
                })
            })
            .collect()
    }

    #[test]
    fn prepared_abs_widens_all_signed_integer_minima_and_preserves_nulls() {
        let cases: Vec<(ArrayRef, DataType, i128, i128)> = vec![
            (
                Arc::new(Int8Array::from(vec![
                    Some(i8::MIN),
                    Some(-7),
                    Some(0),
                    Some(7),
                    Some(i8::MAX),
                    None,
                ])),
                DataType::Int16,
                128,
                i128::from(i8::MAX),
            ),
            (
                Arc::new(Int16Array::from(vec![
                    Some(i16::MIN),
                    Some(-7),
                    Some(0),
                    Some(7),
                    Some(i16::MAX),
                    None,
                ])),
                DataType::Int32,
                32768,
                i128::from(i16::MAX),
            ),
            (
                Arc::new(Int32Array::from(vec![
                    Some(i32::MIN),
                    Some(-7),
                    Some(0),
                    Some(7),
                    Some(i32::MAX),
                    None,
                ])),
                DataType::Int64,
                2147483648,
                i128::from(i32::MAX),
            ),
            (
                Arc::new(Int64Array::from(vec![
                    Some(i64::MIN),
                    Some(-7),
                    Some(0),
                    Some(7),
                    Some(i64::MAX),
                    None,
                ])),
                DataType::FixedSizeBinary(16),
                9223372036854775808,
                i128::from(i64::MAX),
            ),
        ];
        for (input, output, minimum_abs, maximum) in cases {
            let result = prepared_abs(input, output)
                .expect("ABS after widening must represent the signed minimum");
            assert_eq!(
                signed_values(&result),
                [
                    Some(minimum_abs),
                    Some(7),
                    Some(0),
                    Some(7),
                    Some(maximum),
                    None
                ]
            );
        }
    }

    #[test]
    fn prepared_abs_largeint_retains_the_explicit_twos_complement_boundary() {
        let input = largeint::array_from_i128(&[
            Some(i128::MIN),
            Some(-i128::MAX),
            Some(-7),
            Some(0),
            Some(7),
            Some(i128::MAX),
            None,
        ])
        .unwrap();
        let result = prepared_abs(input, DataType::FixedSizeBinary(16)).unwrap();
        assert_eq!(
            signed_values(&result),
            [
                Some(i128::MIN),
                Some(i128::MAX),
                Some(7),
                Some(0),
                Some(7),
                Some(i128::MAX),
                None,
            ]
        );
    }

    #[test]
    fn prepared_abs_same_width_minima_fail_instead_of_returning_null_or_wrapping() {
        for (input, output) in [
            (
                Arc::new(Int8Array::from(vec![Some(7), Some(i8::MIN), None])) as ArrayRef,
                DataType::Int8,
            ),
            (
                Arc::new(Int16Array::from(vec![Some(7), Some(i16::MIN), None])) as ArrayRef,
                DataType::Int16,
            ),
            (
                Arc::new(Int32Array::from(vec![Some(7), Some(i32::MIN), None])) as ArrayRef,
                DataType::Int32,
            ),
            (
                Arc::new(Int64Array::from(vec![Some(7), Some(i64::MIN), None])) as ArrayRef,
                DataType::Int64,
            ),
        ] {
            let error = prepared_abs(input, output)
                .expect_err("an invalid narrow plan must fail checked ABS");
            assert!(error.contains("abs overflow"), "{error}");
        }
    }

    #[test]
    fn prepared_abs_preserves_decimal_and_float_result_types_and_nulls() {
        let input = Arc::new(
            Decimal128Array::from(vec![Some(-12345), Some(0), Some(12345), None])
                .with_precision_and_scale(18, 3)
                .unwrap(),
        ) as ArrayRef;
        let result = prepared_abs(input, DataType::Decimal128(18, 3)).unwrap();
        let decimal = result.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(
            decimal.iter().collect::<Vec<_>>(),
            [Some(12345), Some(0), Some(12345), None]
        );
        let result = prepared_abs(
            Arc::new(Float32Array::from(vec![
                Some(-1.5),
                Some(0.0),
                Some(1.5),
                None,
            ])),
            DataType::Float32,
        )
        .unwrap();
        assert_eq!(
            result
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [Some(1.5), Some(0.0), Some(1.5), None]
        );
        let result = prepared_abs(
            Arc::new(Float64Array::from(vec![
                Some(-1.5),
                Some(0.0),
                Some(1.5),
                None,
            ])),
            DataType::Float64,
        )
        .unwrap();
        assert_eq!(
            result
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [Some(1.5), Some(0.0), Some(1.5), None]
        );
    }
}
