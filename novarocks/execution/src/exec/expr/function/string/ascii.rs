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
#[cfg(test)]
use arrow::array::{Array, StringArray};
#[cfg(test)]
use std::sync::Arc;
pub fn eval_ascii<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let _ = expr;
    let array = arena.eval(args[0], chunk)?;
    Ok((novarocks_functions::builtin::string_measure::evaluate_legacy_ascii(&array))?)
}
#[cfg(test)]
mod legacy_string_measure_contract_tests {
    use super::*;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::expr::ExprNode;
    use crate::exec::expr::function::FunctionKind;
    use arrow::array::{BinaryArray, Int32Array, LargeStringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;

    fn evaluate_with_arity(
        name: &'static str,
        input: ArrayRef,
        arity: usize,
    ) -> Result<ArrayRef, String> {
        let slot = SlotId::new(1);
        let carrier = input.data_type().clone();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "source",
                carrier.clone(),
                true,
            )])),
            vec![input],
        )
        .unwrap();
        let schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[slot])
                .unwrap();
        let chunk = Chunk::new_with_chunk_schema(batch, schema);
        let mut arena = ExprArena::default();
        let source = arena.push_typed(ExprNode::SlotId(slot), carrier);
        // This is the actual old String dispatch ABI with the declared Int32
        // result carrier, not a new full-type or pure-owner receipt.
        let call = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::String(name),
                args: vec![source; arity],
            },
            DataType::Int32,
        );
        let frozen = arena.into_immutable().unwrap();
        let output = ExprArena::from_immutable(&frozen)
            .expect("legacy frozen expression fixture")
            .eval(call, &chunk)?;
        assert_eq!(output.data_type(), &DataType::Int32);
        Ok(output)
    }

    fn evaluate(name: &'static str, input: ArrayRef) -> Vec<Option<i32>> {
        evaluate_with_arity(name, input, 1)
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect()
    }

    #[test]
    fn legacy_string_measure_ascii_returns_first_utf8_byte_and_empty_zero() {
        let input = Arc::new(StringArray::from(vec![
            Some(""),
            Some("A"),
            Some("é"),
            Some("中"),
            Some("😀"),
            Some("\0A"),
            None,
        ])) as ArrayRef;
        assert_eq!(
            evaluate("ascii", input),
            vec![
                Some(0),
                Some(65),
                Some(195),
                Some(228),
                Some(240),
                Some(0),
                None
            ]
        );
    }

    #[test]
    fn legacy_string_measure_bytes_and_unicode_scalars_are_distinct() {
        let input = Arc::new(StringArray::from(vec![
            Some(""),
            Some("ASCII"),
            Some("é"),
            Some("e\u{301}"),
            Some("😀"),
            Some("👩\u{200d}💻"),
            Some("a\0b"),
            None,
        ])) as ArrayRef;
        assert_eq!(
            evaluate("length", input.clone()),
            vec![
                Some(0),
                Some(5),
                Some(2),
                Some(3),
                Some(4),
                Some(11),
                Some(3),
                None
            ]
        );
        assert_eq!(
            evaluate("char_length", input),
            vec![
                Some(0),
                Some(5),
                Some(1),
                Some(2),
                Some(1),
                Some(3),
                Some(3),
                None
            ]
        );
    }

    #[test]
    fn legacy_string_measure_sliced_arrays_use_actual_offsets_and_validity() {
        let pool = Arc::new(StringArray::from(vec![
            Some("unused prefix"),
            Some("é"),
            None,
            Some("👩\u{200d}💻"),
            Some("unused suffix"),
        ])) as ArrayRef;
        let slice = pool.slice(1, 3);
        assert_eq!(
            evaluate("ascii", slice.clone()),
            vec![Some(195), None, Some(240)]
        );
        assert_eq!(
            evaluate("length", slice.clone()),
            vec![Some(2), None, Some(11)]
        );
        assert_eq!(evaluate("char_length", slice), vec![Some(1), None, Some(3)]);
    }

    #[test]
    fn legacy_string_measure_all_null_and_zero_row_batches_preserve_shape() {
        for name in ["ascii", "char_length", "length"] {
            let nulls = Arc::new(StringArray::from(vec![None::<&str>; 3])) as ArrayRef;
            assert_eq!(evaluate(name, nulls), vec![None; 3]);
            let empty = Arc::new(StringArray::from(Vec::<Option<&str>>::new())) as ArrayRef;
            assert!(evaluate(name, empty).is_empty());
        }
    }

    #[test]
    fn legacy_string_measure_single_row_input_is_not_a_broadcast_claim() {
        let input = Arc::new(StringArray::from(vec![Some("中")])) as ArrayRef;
        assert_eq!(evaluate("ascii", input.clone()), vec![Some(228)]);
        assert_eq!(evaluate("length", input.clone()), vec![Some(3)]);
        assert_eq!(evaluate("char_length", input), vec![Some(1)]);
    }

    #[test]
    fn legacy_string_measure_real_catalogue_selects_only_three_fixed_profiles() {
        use novarocks_functions::{
            FunctionArgument, FunctionArgumentType, FunctionBindingError, FunctionBindingRequest,
            FunctionKind as BindingKind, FunctionResultType, FunctionValueType,
        };
        use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
        struct TestControl;
        impl PureCompileControl for TestControl {
            fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
                assert!(units <= 256);
                Ok(())
            }
        }
        let catalog =
            novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog()
                .unwrap();
        for name in ["ascii", "char_length", "length"] {
            for nullable in [false, true] {
                let source = FunctionValueType::new(DataType::Utf8, nullable);
                let arguments = [FunctionArgument::Value {
                    value_type: source.clone(),
                    constant: None,
                }];
                let request = FunctionBindingRequest {
                    arguments: &arguments,
                    expected_result_type: None,
                    logical_argument_count: 1,
                };
                let bound = catalog
                    .resolve_bound_user(name, BindingKind::Scalar, request, &TestControl)
                    .unwrap();
                catalog
                    .validate_bound(&bound, request, &TestControl)
                    .unwrap();
                assert_eq!(
                    bound.function_id.as_str(),
                    format!("builtin.scalar/{name}/v1")
                );
                assert_eq!(
                    bound.selected.overload.as_str(),
                    format!("builtin.scalar/{name}/(utf8)->i32;strict;legacy")
                );
                assert_eq!(
                    bound.selected.argument_types.as_ref(),
                    &[FunctionArgumentType::Value(source)]
                );
                let FunctionResultType::Scalar(result) = &bound.selected.result_type else {
                    panic!("scalar result required");
                };
                assert_eq!(result.data_type().clone(), DataType::Int32);
                assert_eq!(
                    result.logical_type(),
                    novarocks_type_contract::ValueLogicalType::Physical
                );
                // ASCII's existing binding conservatively remains nullable;
                // the two length bindings use source nullability.
                assert_eq!(result.nullable(), name == "ascii" || nullable);
            }
        }
        let arguments = [FunctionArgument::Value {
            value_type: FunctionValueType::new(DataType::Utf8, true),
            constant: None,
        }];
        for name in ["character_length", "octet_length"] {
            assert!(matches!(
                catalog.resolve_bound_user(
                    name,
                    BindingKind::Scalar,
                    FunctionBindingRequest {
                        arguments: &arguments,
                        expected_result_type: None,
                        logical_argument_count: 1,
                    },
                    &TestControl,
                ),
                Err(FunctionBindingError::UnknownFunction)
            ));
        }
    }

    #[test]
    fn legacy_string_measure_dispatch_arity_and_uncoerced_carrier_failures_remain_outer() {
        for name in ["ascii", "char_length", "length"] {
            let input = Arc::new(StringArray::from(vec![Some("x")])) as ArrayRef;
            for arity in [0, 2] {
                let error = evaluate_with_arity(name, input.clone(), arity).unwrap_err();
                assert!(
                    error.contains("expects 1") && error.contains(&format!("got {arity}")),
                    "{error}"
                );
            }
            for input in [
                Arc::new(LargeStringArray::from(vec![Some("x")])) as ArrayRef,
                Arc::new(BinaryArray::from(vec![Some(b"x".as_slice())])) as ArrayRef,
            ] {
                let error = evaluate_with_arity(name, input, 1).unwrap_err();
                assert!(error.contains("expects string"), "{error}");
            }
        }
    }
}
