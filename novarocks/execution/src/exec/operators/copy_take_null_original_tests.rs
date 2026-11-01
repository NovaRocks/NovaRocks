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

//! Logical NULL/value oracles are independent of the Arrow60.1 physical child
//! and run choices. Paired host tests retain the actual SDK result/error and
//! original allocation custody through its final owner.
use super::*;
use arrow::buffer::NullBuffer;
use arrow::datatypes::UnionMode;

fn nullable(raw: Vec<u32>, valid: Vec<bool>) -> Arc<UInt32Array> {
    Arc::new(UInt32Array::new(raw.into(), Some(NullBuffer::from(valid))))
}
fn run_source() -> ArrayRef {
    Arc::new(
        RunArray::<Int16Type>::try_new(
            &Int16Array::from(vec![1, 3]),
            &Int32Array::from(vec![10, 20]),
        )
        .unwrap(),
    )
}
fn union_source(dense: bool, zero_id: bool) -> ArrayRef {
    let ids = if zero_id { [0, 127] } else { [1, 127] };
    let fields = UnionFields::try_new(
        ids,
        [
            Field::new("first", DataType::Int64, false),
            Field::new("last", DataType::Int64, true),
        ],
    )
    .unwrap();
    let (offsets, children): (Option<ScalarBuffer<i32>>, Vec<ArrayRef>) = if dense {
        (
            Some(vec![0_i32, 0, 1].into()),
            vec![
                Arc::new(Int64Array::from(vec![1, 3])),
                Arc::new(Int64Array::from(vec![4])),
            ],
        )
    } else {
        (
            None,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![4, 5, 6])),
            ],
        )
    };
    Arc::new(
        UnionArray::try_new(fields, vec![ids[0], 127, ids[0]].into(), offsets, children).unwrap(),
    )
}

fn logical_union_values(array: &UnionArray) -> Vec<Option<i64>> {
    (0..array.len())
        .map(|row| {
            let child = array
                .child(array.type_id(row))
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let offset = array.value_offset(row);
            child.is_valid(offset).then(|| child.value(offset))
        })
        .collect()
}

#[test]
fn by_original_take_null_run_ignores_hidden_oob_and_represents_logical_null() {
    let source = run_source();
    for hidden in [0, 99, u32::MAX] {
        let indices = nullable(vec![hidden, 2], vec![false, true]);
        let output = arrow::compute::take(source.as_ref(), indices.as_ref(), None).unwrap();
        let output = output
            .as_any()
            .downcast_ref::<RunArray<Int16Type>>()
            .unwrap();
        let values = output
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let actual = (0..output.len())
            .map(|row| {
                let physical = output.get_physical_index(row);
                values.is_valid(physical).then(|| values.value(physical))
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, [None, Some(20)]);
        assert_eq!(output.run_ends().values(), &[1_i16, 2]);
    }
}

#[test]
fn by_original_take_null_union_selects_first_physically_nullable_child_without_id_zero() {
    for dense in [false, true] {
        for zero_id in [false, true] {
            let source = union_source(dense, zero_id);
            let indices = nullable(vec![2, 1, 99], vec![false, true, false]);
            let output = arrow::compute::take(source.as_ref(), indices.as_ref(), None).unwrap();
            let output = output.as_any().downcast_ref::<UnionArray>().unwrap();
            assert_eq!(output.type_ids().as_ref(), &[127_i8, 127, 127]);
            assert_eq!(
                logical_union_values(output),
                [None, Some(if dense { 4 } else { 5 }), None]
            );
            let nonnullable = output
                .child(if zero_id { 0 } else { 1 })
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(nonnullable.null_count(), 0);
            if dense {
                assert!(nonnullable.is_empty());
                assert_eq!(output.offsets().unwrap().as_ref(), &[0_i32, 1, 2]);
            } else {
                // Other sparse children have non-null dummy zero indices;
                // their hidden values do not declare the selected SQL value.
                assert_eq!(nonnullable.values().as_ref(), &[1_i64, 2, 1]);
            }
        }
    }
}

#[test]
fn by_original_take_null_run_merges_equal_values_from_distinct_physical_runs() {
    let source = RunArray::<Int16Type>::try_new(
        &Int16Array::from(vec![1, 2, 3]),
        &Int32Array::from(vec![7, 7, 9]),
    )
    .unwrap();
    let indices = nullable(
        vec![0, 1, 99, u32::MAX, 2],
        vec![true, true, false, false, true],
    );
    let output = arrow::compute::take(&source, indices.as_ref(), None).unwrap();
    let output = output
        .as_any()
        .downcast_ref::<RunArray<Int16Type>>()
        .unwrap();
    assert_eq!(output.run_ends().values(), &[2_i16, 4, 5]);
    let values = output
        .values()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(values.iter().collect::<Vec<_>>(), [Some(7), None, Some(9)]);
}

#[test]
fn by_original_take_null_union_without_nullable_descendant_returns_actual_sdk_error() {
    let fields =
        UnionFields::try_new([5], [Field::new("required", DataType::Int64, false)]).unwrap();
    let source: ArrayRef = Arc::new(
        UnionArray::try_new(
            fields,
            vec![5_i8].into(),
            None,
            vec![Arc::new(Int64Array::from(vec![7]))],
        )
        .unwrap(),
    );
    let indices = nullable(vec![u32::MAX], vec![false]);
    let error = arrow::compute::take(source.as_ref(), indices.as_ref(), None).unwrap_err();
    assert_eq!(
        error.to_string(),
        "Compute error: Cannot take null indices from a union with no field that can represent nulls"
    );
    let tr = tracker();
    let host = Host::new(tr.clone(), None);
    let control = Control::new(tr, None);
    let actual = take_copy_in(
        source,
        CopyIndices::UInt32(indices),
        (),
        host.clone(),
        &control,
    );
    match actual {
        Err(CopyOperationError::OriginalData(actual)) => {
            assert_eq!(actual, error.to_string());
            drop(actual);
        }
        Ok(_) => panic!("actual SDK source must not succeed"),
        Err(other) => panic!("actual SDK source must survive: {other:?}"),
    }
    assert_released(&host);
}

#[test]
fn by_copy_joint_take_null_actual_original_success_and_data_paths() {
    for (source, indices) in [
        (run_source(), nullable(vec![0, 2], vec![false, true])),
        (run_source(), nullable(vec![99, 0], vec![false, true])),
        (
            union_source(false, true),
            nullable(vec![2, 1, 99], vec![false, true, false]),
        ),
        (
            union_source(true, true),
            nullable(vec![2, 1, 99], vec![false, true, false]),
        ),
        (union_source(false, false), nullable(vec![99], vec![false])),
    ] {
        let expected = arrow::compute::take(source.as_ref(), indices.as_ref(), None);
        let tr = tracker();
        let host = Host::new(tr.clone(), None);
        let control = Control::new(tr, None);
        let actual = take_copy_in(
            source,
            CopyIndices::UInt32(indices),
            (),
            host.clone(),
            &control,
        );
        match (expected, actual) {
            (Ok(expected), Ok(actual)) => {
                assert_eq!(actual.values().to_data(), expected.to_data());
                drop(actual);
            }
            (Err(expected), Err(CopyOperationError::OriginalData(actual))) => {
                assert_eq!(actual, expected.to_string());
                drop(actual);
            }
            (expected, actual) => panic!(
                "original/hosted NULL-index mismatch: expected={expected:?}, actual={}",
                match actual {
                    Ok(_) => "success".into(),
                    Err(error) => format!("{error:?}"),
                }
            ),
        }
        assert_released(&host);
    }
}

#[test]
fn by_copy_joint_nullable_uint64_ignores_hidden_max_index_with_original_custody() {
    for source in [
        run_source(),
        union_source(false, false),
        union_source(true, false),
    ] {
        let indices = Arc::new(UInt64Array::new(
            vec![u64::MAX, 1, u64::MAX].into(),
            Some(NullBuffer::from(vec![false, true, false])),
        ));
        let expected = arrow::compute::take(source.as_ref(), indices.as_ref(), None).unwrap();
        let tr = tracker();
        let host = Host::new(tr.clone(), None);
        let control = Control::new(tr, None);
        let actual = take_copy_in(
            source,
            CopyIndices::UInt64(indices),
            (),
            host.clone(),
            &control,
        )
        .unwrap();
        assert_eq!(actual.values().to_data(), expected.to_data());
        drop(actual);
        assert_released(&host);
    }
}

#[test]
fn by_copy_joint_union_null_selection_skips_nullable_field_without_nullable_descendant() {
    let inner_fields =
        UnionFields::try_new([9], [Field::new("required", DataType::Int64, false)]).unwrap();
    let inner: ArrayRef = Arc::new(
        UnionArray::try_new(
            inner_fields,
            vec![9_i8].into(),
            None,
            vec![Arc::new(Int64Array::from(vec![7]))],
        )
        .unwrap(),
    );
    let fields = UnionFields::try_new(
        [5, 127],
        [
            Field::new("nullable_parent", inner.data_type().clone(), true),
            Field::new("actual_null_child", DataType::Int64, true),
        ],
    )
    .unwrap();
    let source: ArrayRef = Arc::new(
        UnionArray::try_new(
            fields,
            vec![5_i8, 127].into(),
            Some(vec![0_i32, 0].into()),
            vec![inner, Arc::new(Int64Array::from(vec![11]))],
        )
        .unwrap(),
    );
    let indices = nullable(vec![u32::MAX, 0], vec![false, true]);
    let expected = arrow::compute::take(source.as_ref(), indices.as_ref(), None).unwrap();
    let union = expected.as_any().downcast_ref::<UnionArray>().unwrap();
    assert_eq!(union.type_ids().as_ref(), &[127_i8, 5]);
    assert!(union.child(127).is_null(0));
    let tr = tracker();
    let host = Host::new(tr.clone(), None);
    let control = Control::new(tr, None);
    let actual = take_copy_in(
        source,
        CopyIndices::UInt32(indices),
        (),
        host.clone(),
        &control,
    )
    .unwrap();
    assert_eq!(actual.values().to_data(), expected.to_data());
    drop(actual);
    assert_released(&host);
}

#[test]
fn by_copy_joint_empty_sparse_unselected_struct_preserves_full_sdk_validation_error() {
    let long_name = "original field Unicode 窗口 with metadata ".repeat(80);
    let field = Arc::new(
        Field::new(long_name.clone(), DataType::Utf8, false).with_metadata(
            std::collections::HashMap::<String, String>::from([(
                "source".to_string(),
                "metadata payload".repeat(80),
            )]),
        ),
    );
    let empty: ArrayRef = Arc::new(
        arrow::array::StructArray::try_new(
            vec![field].into(),
            vec![Arc::new(
                arrow::array::StringArray::from(Vec::<&str>::new()),
            )],
            None,
        )
        .unwrap(),
    );
    let fields = UnionFields::try_new(
        [5, 127],
        [
            Field::new("unselected", empty.data_type().clone(), false),
            Field::new("selected_null", DataType::Int64, true),
        ],
    )
    .unwrap();
    let source: ArrayRef = Arc::new(
        UnionArray::try_new(
            fields,
            Vec::<i8>::new().into(),
            None,
            vec![empty, Arc::new(Int64Array::from(Vec::<i64>::new()))],
        )
        .unwrap(),
    );
    let indices = nullable(vec![u32::MAX], vec![false]);
    let expected = arrow::compute::take(source.as_ref(), indices.as_ref(), None).unwrap_err();
    let text = expected.to_string();
    assert!(text.contains("non-nullable child"));
    assert!(text.contains(&long_name));
    assert!(
        text.len() > 512,
        "original SDK detail has no arbitrary message cap"
    );
    let tr = tracker();
    let host = Host::new(tr.clone(), None);
    let control = Control::new(tr, None);
    match take_copy_in(
        source,
        CopyIndices::UInt32(indices),
        (),
        host.clone(),
        &control,
    ) {
        Err(CopyOperationError::OriginalData(actual)) => {
            assert_eq!(actual.text(), text);
            drop(actual);
        }
        Ok(_) => panic!("new validated child must retain original SDK error"),
        Err(cause) => panic!("original SDK error lost: {cause:?}"),
    }
    assert_released(&host);
}

#[test]
fn by_copy_joint_run_nonnullable_values_retains_actual_sdk_compute_error() {
    let ty = DataType::RunEndEncoded(
        Arc::new(Field::new("run_ends", DataType::Int16, false)),
        Arc::new(Field::new("values", DataType::Int32, false)),
    );
    let data = arrow::array::ArrayData::builder(ty)
        .len(3)
        .child_data(vec![
            Int16Array::from(vec![1, 3]).to_data(),
            Int32Array::from(vec![10, 20]).to_data(),
        ])
        .build()
        .unwrap();
    let source = arrow::array::make_array(data);
    let indices = nullable(vec![u32::MAX], vec![false]);
    let expected = arrow::compute::take(source.as_ref(), indices.as_ref(), None).unwrap_err();
    assert_eq!(
        expected.to_string(),
        "Compute error: Cannot take null indices from a RunEndEncoded array with a non-nullable values field"
    );
    let tr = tracker();
    let host = Host::new(tr.clone(), None);
    let control = Control::new(tr, None);
    match take_copy_in(
        source,
        CopyIndices::UInt32(indices),
        (),
        host.clone(),
        &control,
    ) {
        Err(CopyOperationError::OriginalData(actual)) => {
            assert_eq!(actual, expected.to_string());
            drop(actual);
        }
        Ok(_) => panic!("nonnullable values must retain SDK refusal"),
        Err(cause) => panic!("SDK refusal replaced: {cause:?}"),
    }
    assert_released(&host);
}

#[test]
fn by_copy_joint_empty_sparse_unselected_fieldless_union_keeps_actual_sdk_panic() {
    let empty =
        arrow::array::new_empty_array(&DataType::Union(UnionFields::empty(), UnionMode::Sparse));
    let fields = UnionFields::try_new(
        [5, 127],
        [
            Field::new("unselected", empty.data_type().clone(), false),
            Field::new("selected_null", DataType::Int64, true),
        ],
    )
    .unwrap();
    let source: ArrayRef = Arc::new(
        UnionArray::try_new(
            fields,
            Vec::<i8>::new().into(),
            None,
            vec![empty, Arc::new(Int64Array::from(Vec::<i64>::new()))],
        )
        .unwrap(),
    );
    let indices = nullable(vec![u32::MAX], vec![false]);
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| arrow::compute::take(
            source.as_ref(),
            indices.as_ref(),
            None
        )))
        .is_err()
    );
    let tr = tracker();
    let host = Host::new(tr.clone(), None);
    let control = Control::new(tr, None);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        take_copy_in(
            source,
            CopyIndices::UInt32(indices),
            (),
            host.clone(),
            &control,
        )
    }));
    assert!(
        result.is_err(),
        "a source-model rejection must not replace the original SDK panic"
    );
    assert_released(&host);
}

#[test]
fn by_copy_joint_sparse_dummy_zero_payload_is_paid_even_when_logical_result_is_null() {
    let payload = "unselected but physically copied UTF8 窗口".repeat(1024);
    let strings: ArrayRef = Arc::new(arrow::array::StringArray::from(vec![payload.as_str()]));
    let fields = UnionFields::try_new(
        [5, 127],
        [
            Field::new("required", DataType::Utf8, false),
            Field::new("nullable", DataType::Int64, true),
        ],
    )
    .unwrap();
    let source: ArrayRef = Arc::new(
        UnionArray::try_new(
            fields,
            vec![5_i8].into(),
            None,
            vec![strings, Arc::new(Int64Array::from(vec![7]))],
        )
        .unwrap(),
    );
    let indices = nullable(vec![u32::MAX], vec![false]);
    let tr = tracker();
    let host = Host::new(tr.clone(), None);
    let control = Control::new(tr, None);
    let actual = take_copy_in(
        source,
        CopyIndices::UInt32(indices),
        (),
        host.clone(),
        &control,
    )
    .unwrap();
    let output = actual
        .values()
        .as_any()
        .downcast_ref::<UnionArray>()
        .unwrap();
    assert_eq!(output.type_ids().as_ref(), &[127_i8]);
    assert!(output.child(127).is_null(0));
    assert_eq!(
        output
            .child(5)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap()
            .value(0),
        payload
    );
    assert!(actual.facts().retained_new_backing_upper() >= payload.len());
    drop(actual);
    assert_released(&host);
}
