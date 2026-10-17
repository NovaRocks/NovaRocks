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

//! Frozen path/type values retain the original literal-only parameter domain.

use super::*;
use crate::exec::chunk::ChunkSchema;
use arrow::array::{Int32Array, Int64Array, LargeStringArray, NullArray};
use arrow::datatypes::Schema;
use novarocks_functions::{ConstantPolicy, ConstantPool, ConstantValue};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, FunctionValueType, PureCompileControl, ValueLogicalType,
};
use novarocks_types::SlotId;

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
fn selected_typed(array: ArrayRef, ty: FunctionValueType) -> ConstantValue {
    assert_eq!(array.len(), 3);
    let field = Arc::new(ty.try_to_field("frozen parameter").unwrap());
    let pool = ConstantPool::try_new(
        field,
        ty,
        array.to_data(),
        ConstantPolicy {
            max_rows: 3,
            max_array_nodes: 16,
            max_logical_elements: 64,
            max_retained_buffer_bytes: 1 << 16,
            max_type_depth: 8,
            max_type_nodes: 32,
            max_dictionary_depth: 4,
            max_metadata_bytes: 1 << 16,
            max_library_validation_work: 1 << 16,
            max_library_validation_bytes: 1 << 16,
        },
        CompilePhase::Validate,
        &Control,
    )
    .unwrap();
    let value = pool.value(1).unwrap();
    assert_eq!(value.ordinal(), 1);
    value
}
fn selected(array: ArrayRef) -> ConstantValue {
    let ty = FunctionValueType::new(array.data_type().clone(), array.logical_null_count() != 0);
    selected_typed(array, ty)
}
fn string_parameter(value: &str) -> ConstantValue {
    selected(Arc::new(StringArray::from(vec![
        "poison_not_the_parameter",
        value,
        "another_poison",
    ])))
}
fn parameter(arena: &mut ExprArena, value: ConstantValue) -> ExprId {
    let ty = value.value_type().clone();
    arena.push_value_typed(ExprNode::Constant(value), ty)
}
fn chunk(array: ArrayRef) -> Chunk {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "json",
        array.data_type().clone(),
        true,
    )]));
    let batch = arrow::record_batch::RecordBatch::try_new(schema, vec![array]).unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[SlotId::new(1)])
            .unwrap();
    Chunk::try_new_with_chunk_schema(batch, schema).unwrap()
}
type Evaluate = fn(&ExprArena, ExprId, &[ExprId], &Chunk) -> Result<ArrayRef, String>;
const FUNCTIONS: [(&str, Evaluate); 2] = [
    ("variant_get", eval_variant_get),
    ("try_variant_get", eval_try_variant_get),
];

#[test]
fn variant_get_frozen_path_and_type_read_nonzero_pool_ordinal_for_both_public_functions() {
    for (_, evaluate) in FUNCTIONS {
        let mut arena = ExprArena::default();
        let input = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
        let path = parameter(&mut arena, string_parameter("$.nested.a"));
        let ty = parameter(&mut arena, string_parameter("int"));
        let source = chunk(Arc::new(StringArray::from(vec![
            Some(r#"{"nested":{"a":42}}"#),
            Some(r#"{}"#),
            Some(r#"{"nested":{"a":7}}"#),
        ])));
        let result = evaluate(&arena, input, &[input, path, ty], &source).unwrap();
        assert_eq!(result.data_type(), &DataType::Int32);
        assert_eq!(
            result
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(42), None, Some(7)]
        );
    }
}

#[test]
fn variant_get_legacy_and_frozen_parameters_share_the_same_path_and_type_author() {
    for (_, evaluate) in FUNCTIONS {
        for frozen_path in [false, true] {
            let mut arena = ExprArena::default();
            let input = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
            let path = if frozen_path {
                parameter(&mut arena, string_parameter("$.a"))
            } else {
                arena.push_typed(
                    ExprNode::Literal(LiteralValue::Utf8("$.a".into())),
                    DataType::Utf8,
                )
            };
            let ty = if frozen_path {
                arena.push_typed(
                    ExprNode::Literal(LiteralValue::Utf8("int".into())),
                    DataType::Utf8,
                )
            } else {
                parameter(&mut arena, string_parameter("int"))
            };
            let source = chunk(Arc::new(StringArray::from(vec![
                Some(r#"{"a":42}"#),
                None,
                Some(r#"{"a":7}"#),
            ])));
            let result = evaluate(&arena, input, &[input, path, ty], &source).unwrap();
            assert_eq!(
                result
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![Some(42), None, Some(7)]
            );
        }
    }
}

#[test]
fn variant_get_two_argument_form_keeps_original_variant_return_type() {
    for (_, evaluate) in FUNCTIONS {
        let mut arena = ExprArena::default();
        let input = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
        let path = parameter(&mut arena, string_parameter("$.a"));
        let source = chunk(Arc::new(StringArray::from(vec![
            Some(r#"{"a":42}"#),
            Some(r#"{}"#),
            None,
        ])));
        let result = evaluate(&arena, input, &[input, path], &source).unwrap();
        let values = result.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
        assert_eq!(
            VariantValue::from_serialized(values.value(0))
                .unwrap()
                .to_json(None)
                .unwrap(),
            "42"
        );
        assert!(values.is_null(1));
        assert!(values.is_null(2));
    }
}

fn rejected_constants() -> Vec<ConstantValue> {
    let json =
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap();
    vec![
        selected(Arc::new(StringArray::from(vec![
            Some("$.wrong"),
            None,
            Some("$.also_wrong"),
        ]))),
        selected(Arc::new(NullArray::new(3))),
        selected(Arc::new(Int64Array::from(vec![0, 1, 2]))),
        selected(Arc::new(LargeStringArray::from(vec![
            "$.wrong", "$.a", "$.wrong",
        ]))),
        selected_typed(
            Arc::new(StringArray::from(vec![
                r#""wrong""#,
                r#""$.a""#,
                r#""wrong""#,
            ])),
            json,
        ),
    ]
}
#[test]
fn variant_get_frozen_null_nominal_and_wrong_types_still_refuse_before_input_evaluation() {
    for (name, evaluate) in FUNCTIONS {
        for what in ["path", "type"] {
            for value in rejected_constants() {
                let mut arena = ExprArena::default();
                let missing = arena.push_typed(ExprNode::SlotId(SlotId::new(777)), DataType::Utf8);
                let valid_path = parameter(&mut arena, string_parameter("$.a"));
                let invalid = parameter(&mut arena, value);
                let args = if what == "path" {
                    vec![missing, invalid]
                } else {
                    vec![missing, valid_path, invalid]
                };
                let source = chunk(Arc::new(StringArray::from(vec!["$", "$", "$"])));
                assert_eq!(
                    evaluate(&arena, missing, &args, &source).unwrap_err(),
                    format!("{name} requires a constant string literal for the {what} argument")
                );
            }
        }
    }
}

#[test]
fn variant_get_dynamic_slot_and_foldable_cast_do_not_become_literal_parameters() {
    for (name, evaluate) in FUNCTIONS {
        for what in ["path", "type"] {
            for cast in [false, true] {
                let mut arena = ExprArena::default();
                let missing = arena.push_typed(ExprNode::SlotId(SlotId::new(777)), DataType::Utf8);
                let dynamic = if cast {
                    let constant = parameter(&mut arena, string_parameter("$.a"));
                    arena.push_typed(
                        ExprNode::Cast(
                            constant,
                            novarocks_type_contract::DecimalOverflowPolicy::ReportError,
                        ),
                        DataType::Utf8,
                    )
                } else {
                    arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8)
                };
                let valid_path = parameter(&mut arena, string_parameter("$.a"));
                let args = if what == "path" {
                    vec![missing, dynamic]
                } else {
                    vec![missing, valid_path, dynamic]
                };
                let source = chunk(Arc::new(StringArray::from(vec!["$", "$", "$"])));
                assert_eq!(
                    evaluate(&arena, missing, &args, &source).unwrap_err(),
                    format!("{name} requires a constant string literal for the {what} argument")
                );
            }
        }
    }
}

#[test]
fn variant_get_frozen_string_preserves_selected_utf8_bytes_without_trimming() {
    let mut arena = ExprArena::default();
    let chosen = "$.海\0suffix";
    let value = parameter(&mut arena, string_parameter(chosen));
    assert_eq!(
        literal_utf8_arg(&arena, value, "path", "variant_get").unwrap(),
        chosen
    );
}

#[test]
fn variant_get_frozen_parameters_keep_strict_and_try_cast_disposition() {
    let mut arena = ExprArena::default();
    let input = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
    let path = parameter(&mut arena, string_parameter("$.a"));
    let ty = parameter(&mut arena, string_parameter("int"));
    let source = chunk(Arc::new(StringArray::from(vec![
        r#"{"a":42}"#,
        r#"{"a":"not_an_integer"}"#,
    ])));
    let error = eval_variant_get(&arena, input, &[input, path, ty], &source).unwrap_err();
    assert!(
        error.starts_with("variant_get: "),
        "the original strict kernel, not literal admission, owns this cast failure: {error}"
    );
    let result = eval_try_variant_get(&arena, input, &[input, path, ty], &source).unwrap();
    assert_eq!(
        result
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(42), None]
    );
}
