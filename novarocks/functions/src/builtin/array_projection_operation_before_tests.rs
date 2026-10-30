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
//! Independent original operation witnesses, before any funding adapter.
use super::project;
use arrow_array::types::Int32Type;
use arrow_array::{
    Array, ArrayRef, Int32Array, ListArray, RunArray, StringArray, StructArray, UInt32Array,
    UnionArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, UnionFields};
use std::sync::Arc;

fn input(child: ArrayRef, valid: Vec<bool>) -> ArrayRef {
    let n = valid.len();
    let values = Arc::new(StructArray::new(
        vec![Arc::new(Field::new(
            "item",
            child.data_type().clone(),
            true,
        ))]
        .into(),
        vec![child],
        Some(NullBuffer::from(valid)),
    ));
    Arc::new(ListArray::new(
        Arc::new(Field::new("entry", values.data_type().clone(), true)),
        OffsetBuffer::new((0..=n as i32).collect::<Vec<_>>().into()),
        values,
        None,
    ))
}
#[test]
fn array_operation_original_run_null_indices_keep_nulls_without_hidden_payload_copy() {
    let strings = StringArray::from(vec!["x".repeat(1001), "z".to_string()]);
    let ends = Int32Array::from(vec![4, 8]);
    let run = Arc::new(RunArray::<Int32Type>::try_new(&ends, &strings).unwrap());
    let source_bytes = strings.value_data().len();
    let child: ArrayRef = run.clone();
    let input = input(
        child.clone(),
        vec![false, true, false, true, false, true, false, true],
    );
    let names: ArrayRef = Arc::new(StringArray::from(vec!["item"; 8]));
    let target = DataType::List(Arc::new(Field::new(
        "selected",
        run.data_type().clone(),
        true,
    )));
    let out = project(&input, &names, Some(&target)).unwrap();
    let out = out.as_any().downcast_ref::<ListArray>().unwrap();
    let run = out
        .values()
        .as_any()
        .downcast_ref::<RunArray<Int32Type>>()
        .unwrap();
    let strings = run.values().as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(run.run_ends().values(), &[1, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(
        strings.iter().collect::<Vec<_>>(),
        vec![
            None,
            Some("x".repeat(1001).as_str()),
            None,
            Some("x".repeat(1001).as_str()),
            None,
            Some("z"),
            None,
            Some("z")
        ]
    );
    assert!(strings.value_data().len() <= 2 * source_bytes);
    let indices = UInt32Array::from(vec![
        None,
        Some(1),
        None,
        Some(3),
        None,
        Some(5),
        None,
        Some(7),
    ]);
    assert_eq!(indices.values().as_ref(), &[0, 1, 0, 3, 0, 5, 0, 7]);
    let old = crate::selected_copy::preflight_take(
        child.as_ref(),
        &[None, Some(1), None, Some(3), None, Some(5), None, Some(7)],
        |_| Ok(()),
    );
    assert_eq!(
        old.unwrap_err().to_string(),
        "Arrow run-end take requires non-null indices"
    );
}
#[test]
fn array_operation_original_sparse_union_null_indices_keep_original_type_ids_and_child_nulls() {
    let fields =
        UnionFields::try_new(vec![5], vec![Field::new("item", DataType::Int32, true)]).unwrap();
    let union = Arc::new(
        UnionArray::try_new(
            fields,
            ScalarBuffer::from(vec![5i8; 4]),
            None,
            vec![Arc::new(Int32Array::from(vec![11, 22, 33, 44]))],
        )
        .unwrap(),
    );
    let child: ArrayRef = union.clone();
    let input = input(child.clone(), vec![false, true, false, true]);
    let names: ArrayRef = Arc::new(StringArray::from(vec!["item"; 4]));
    let target = DataType::List(Arc::new(Field::new(
        "selected",
        union.data_type().clone(),
        true,
    )));
    let out = project(&input, &names, Some(&target)).unwrap();
    let out = out.as_any().downcast_ref::<ListArray>().unwrap();
    let union = out.values().as_any().downcast_ref::<UnionArray>().unwrap();
    assert_eq!(union.type_ids().as_ref(), &[5, 5, 5, 5]);
    let child = union
        .child(5)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(
        child.iter().collect::<Vec<_>>(),
        vec![None, Some(22), None, Some(44)]
    );
    let original =
        crate::selected_copy::preflight_take(union, &[None, Some(1), None, Some(3)], |_| Ok(()));
    assert_eq!(
        original.unwrap_err().to_string(),
        "Arrow union take requires non-null indices"
    );
}

#[test]
fn array_operation_case_collision_is_rejected_before_any_output_conversion() {
    use crate::{
        ConstantPolicy, ConstantValue, FunctionArgument, FunctionBindingRequest, FunctionKind,
        FunctionValueType,
    };
    use arrow_array::Int64Array;
    use novarocks_type_contract::CompilePhase;
    let values = Arc::new(StructArray::new(
        vec![
            Arc::new(Field::new("ID", DataType::Int32, false)),
            Arc::new(Field::new("id", DataType::Int64, false)),
        ]
        .into(),
        vec![
            Arc::new(Int32Array::from(vec![0; 3])),
            Arc::new(Int64Array::from(vec![123, i32::MAX as i64 + 1, i64::MIN])),
        ],
        None,
    ));
    let input: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("entry", values.data_type().clone(), true)),
        OffsetBuffer::new(vec![0, 1, 2, 3].into()),
        values,
        None,
    ));
    let constant = ConstantValue::from_utf8(
        Arc::new(Field::new("literal", DataType::Utf8, false)),
        FunctionValueType::new(DataType::Utf8, false),
        "id",
        ConstantPolicy {
            max_rows: 8,
            max_array_nodes: 16,
            max_logical_elements: 64,
            max_retained_buffer_bytes: 1 << 20,
            max_type_depth: 16,
            max_type_nodes: 64,
            max_dictionary_depth: 8,
            max_metadata_bytes: 65536,
            max_library_validation_work: 4 << 20,
            max_library_validation_bytes: 4 << 20,
        },
        CompilePhase::FunctionSpecialization,
        crate::binding_test_control(),
    )
    .unwrap();
    let arguments = [
        FunctionArgument::Value {
            value_type: FunctionValueType::from_legacy_declared_projection(
                input.data_type().clone(),
                false,
            ),
            constant: None,
        },
        FunctionArgument::Value {
            value_type: FunctionValueType::new(DataType::Utf8, false),
            constant: Some(constant),
        },
    ];
    let catalogue = crate::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let error = catalogue
        .resolve_bound_user(
            "__array_struct_subfield",
            FunctionKind::Scalar,
            FunctionBindingRequest {
                arguments: &arguments,
                logical_argument_count: 2,
                expected_result_type: None,
            },
            crate::binding_test_control(),
        )
        .unwrap_err();
    assert!(
        matches!(&error, crate::FunctionBindingError::InvalidBinding(message) if message.contains("ambiguous"))
    );
    let names: ArrayRef = Arc::new(StringArray::from(vec!["id"; 3]));
    let direct = project(&input, &names, None).unwrap_err();
    assert!(direct.to_string().contains("ambiguous"));
    let mut source: &dyn std::error::Error = &direct;
    loop {
        if let Some(crate::FunctionBindingError::InvalidBinding(detail)) =
            source.downcast_ref::<crate::FunctionBindingError>()
        {
            assert!(detail.contains("ambiguous"));
            break;
        }
        source = source
            .source()
            .expect("binding ambiguity must remain discoverable");
    }
}
