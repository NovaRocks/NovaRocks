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

use super::*;
use crate::control_values::{AssemblyPlan, assemble_values};
use arrow_array::{BooleanArray, Int64Array, ListArray, StringArray};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::Field;
use std::sync::Arc;

struct Control;
impl crate::KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("interleave must not wait")
    }
}
fn list_struct_sources() -> Vec<ArrayRef> {
    let fields: arrow_schema::Fields = vec![
        Arc::new(Field::new("user_field", DataType::Utf8, true)),
        Arc::new(Field::new("number", DataType::Int64, true)),
    ]
    .into();
    let first_child: ArrayRef = Arc::new(StructArray::new(
        fields.clone(),
        vec![
            Arc::new(StringArray::from(vec![
                "outside",
                "NULL-first",
                "NULL-second",
                "tail",
            ])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
        ],
        Some(NullBuffer::from(vec![true, true, false, true])),
    ));
    let field = Arc::new(Field::new("item", first_child.data_type().clone(), true));
    let first: ArrayRef = Arc::new(ListArray::new(
        field.clone(),
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1, 3, 4])),
        first_child,
        Some(NullBuffer::from(vec![true, false, true])),
    ));
    let second_child: ArrayRef = Arc::new(StructArray::new(
        fields,
        vec![
            Arc::new(StringArray::from(vec!["other"])),
            Arc::new(Int64Array::from(vec![99])),
        ],
        None,
    ));
    vec![
        first.slice(1, 2),
        Arc::new(ListArray::new(
            field,
            OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1])),
            second_child,
            None,
        )),
    ]
}
#[test]
fn nested_interleave_list_struct_keeps_sliced_offsets_null_parent_spans_and_raw_struct_children() {
    let sources = list_struct_sources();
    let ty = sources[0].data_type();
    let choices = [(0, 1), (1, 0), (0, 0), (0, 1)];
    assert!(supports_guarded_result(ty));
    super::super::preflight_guarded_interleave(ty, &sources, &choices, |_| Ok(())).unwrap();
    let borrowed: Vec<&dyn Array> = sources.iter().map(AsRef::as_ref).collect();
    let output = arrow_select::interleave::interleave(&borrowed, &choices).unwrap();
    assert_eq!(output.data_type(), ty);
    let output = output.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(output.value_offsets(), &[0, 1, 2, 4, 5]);
    assert_eq!(
        (0..4).map(|row| output.is_valid(row)).collect::<Vec<_>>(),
        vec![true, true, false, true]
    );
    let child = output
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(
        (0..5).map(|row| child.is_valid(row)).collect::<Vec<_>>(),
        vec![true, true, true, false, true]
    );
    let strings = child
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        (0..5).map(|row| strings.value(row)).collect::<Vec<_>>(),
        vec!["tail", "other", "NULL-first", "NULL-second", "tail"]
    );
    assert_eq!(
        child
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[40, 99, 20, 30, 40]
    );
}
fn struct_list_sources() -> Vec<ArrayRef> {
    let field = Arc::new(Field::new("item", DataType::Utf8, true));
    let first_list: ArrayRef = Arc::new(ListArray::new(
        field.clone(),
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 2, 3])),
        Arc::new(StringArray::from(vec!["a", "b", "NULL-payload"])),
        None,
    ));
    let second_list: ArrayRef = Arc::new(ListArray::new(
        field,
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1, 2])),
        Arc::new(StringArray::from(vec!["unused", "c"])),
        None,
    ));
    let fields: arrow_schema::Fields = vec![
        Arc::new(Field::new(
            "nested_user_list",
            first_list.data_type().clone(),
            true,
        )),
        Arc::new(Field::new("flag", DataType::Boolean, true)),
    ]
    .into();
    vec![
        Arc::new(StructArray::new(
            fields.clone(),
            vec![first_list, Arc::new(BooleanArray::from(vec![true, false]))],
            Some(NullBuffer::from(vec![true, false])),
        )),
        Arc::new(StructArray::new(
            fields,
            vec![second_list, Arc::new(BooleanArray::from(vec![false, true]))],
            None,
        )),
    ]
}
#[test]
fn indexed_struct_list_assembly_keeps_typed_null_sentinel_and_raw_null_parent_fields() {
    let sources = struct_list_sources();
    let slots = [Some(&sources[0]), Some(&sources[1])];
    let output = assemble_values(
        AssemblyPlan::Indexed {
            result_type: sources[0].data_type(),
            sources: &slots,
            choices: &[Some((0, 0)), None, Some((1, 1)), Some((0, 1))],
        },
        &Control,
    )
    .unwrap();
    let output = output.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(
        (0..4).map(|row| output.is_valid(row)).collect::<Vec<_>>(),
        vec![true, false, true, false]
    );
    assert_eq!(output.fields()[0].name(), "nested_user_list");
    let lists = output
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(lists.value_offsets(), &[0, 2, 2, 3, 4]);
    assert_eq!(
        (0..4).map(|row| lists.is_valid(row)).collect::<Vec<_>>(),
        vec![true, false, true, true]
    );
    let strings = lists
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        strings.iter().collect::<Vec<_>>(),
        vec![Some("a"), Some("b"), Some("c"), Some("NULL-payload")]
    );
    assert_eq!(
        output
            .column(1)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(true), None, Some(true), Some(false)]
    );
}
#[test]
fn nested_indexed_empty_output_keeps_complete_type_and_real_empty_child_offsets() {
    let sources = list_struct_sources();
    let output = assemble_values(
        AssemblyPlan::Indexed {
            result_type: sources[0].data_type(),
            sources: &[Some(&sources[0])],
            choices: &[],
        },
        &Control,
    )
    .unwrap();
    assert_eq!(output.data_type(), sources[0].data_type());
    let list = output.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(list.value_offsets(), &[0]);
    let child = list
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(child.len(), 0);
    assert_eq!(
        child
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value_offsets(),
        &[0]
    );
}
#[test]
fn nested_interleave_rejects_exact_type_source_choice_and_static_extent_failures() {
    let sources = list_struct_sources();
    for choices in [&[(2, 0)][..], &[(0, 2)][..], &[(0, usize::MAX)][..]] {
        assert!(matches!(
            super::super::preflight_guarded_interleave(
                sources[0].data_type(),
                &sources,
                choices,
                |_| Ok(())
            ),
            Err(CopyError::Invalid(_))
        ));
    }
    let renamed = DataType::List(Arc::new(Field::new(
        "wrong_list_name",
        if let DataType::List(field) = sources[0].data_type() {
            field.data_type().clone()
        } else {
            unreachable!()
        },
        true,
    )));
    assert!(matches!(
        super::super::preflight_guarded_interleave(&renamed, &sources, &[(0, 0)], |_| Ok(())),
        Err(CopyError::Invalid(_))
    ));
    assert!(matches!(
        guarded_result_extent(sources[0].data_type(), usize::MAX),
        Err(CopyError::Extent)
    ));
    let struct_type =
        DataType::Struct(vec![Arc::new(Field::new("wide", DataType::Int64, true))].into());
    assert!(matches!(
        guarded_result_extent(&struct_type, isize::MAX as usize / 8 + 1),
        Err(CopyError::Extent)
    ));
}
#[test]
fn nested_guarded_carriers_do_not_admit_other_encoding_or_child_protocols() {
    for ty in [
        DataType::Null,
        DataType::FixedSizeBinary(16),
        DataType::List(Arc::new(Field::new("item", DataType::Utf8View, true))),
        DataType::Struct(
            vec![Arc::new(Field::new(
                "encoded",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                true,
            ))]
            .into(),
        ),
        DataType::LargeList(Arc::new(Field::new("item", DataType::Int64, true))),
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("key", DataType::Utf8, false)),
                        Arc::new(Field::new("value", DataType::Int64, true)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        ),
    ] {
        assert!(!crate::control_values::supports_indexed_result(&ty));
        assert!(matches!(
            guarded_result_extent(&ty, 1),
            Err(CopyError::Unsupported(_))
        ));
        assert!(
            assemble_values(
                AssemblyPlan::Indexed {
                    result_type: &ty,
                    sources: &[],
                    choices: &[None]
                },
                &Control
            )
            .is_err()
        );
    }
}
#[test]
fn every_nested_interleave_callback_refusal_preserves_primary_cause_without_tail() {
    let sources = list_struct_sources();
    let choices = [(0, 1), (1, 0), (0, 0), (0, 1)];
    let mut calls = 0;
    super::super::preflight_guarded_interleave(sources[0].data_type(), &sources, &choices, |_| {
        calls += 1;
        Ok(())
    })
    .unwrap();
    assert!(
        calls > 30,
        "the actual nested source/offset/child walk must be observed"
    );
    for cause in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(crate::KernelDiagnostic::new("original invalid")),
        KernelFailure::Internal(crate::KernelDiagnostic::new("original internal")),
        KernelFailure::Operational(crate::KernelDiagnostic::new("original operation")),
        KernelFailure::InstanceFailed,
    ] {
        for fail_at in 1..=calls {
            let mut seen = 0;
            let result = super::super::preflight_guarded_interleave(
                sources[0].data_type(),
                &sources,
                &choices,
                |_| {
                    seen += 1;
                    if seen == fail_at {
                        Err(cause.clone())
                    } else {
                        Ok(())
                    }
                },
            );
            assert!(
                matches!(result,Err(CopyError::Control(ref actual)) if *actual==cause),
                "cause={cause:?}, callback={fail_at}, result={result:?}"
            );
            assert_eq!(
                seen, fail_at,
                "a refusal must not trigger a footer or another nested callback"
            );
        }
    }
}
#[test]
fn legacy_nested_fill_nulls_keeps_its_original_arrow_zip_path() {
    let field = Arc::new(Field::new("item", DataType::Utf8, true));
    let left: ArrayRef = Arc::new(ListArray::new(
        field.clone(),
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1, 1, 2])),
        Arc::new(StringArray::from(vec!["a", "b"])),
        Some(NullBuffer::from(vec![true, false, true])),
    ));
    let right: ArrayRef = Arc::new(ListArray::new(
        field,
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1, 3, 4])),
        Arc::new(StringArray::from(vec!["unused", "y", "z", "also-unused"])),
        None,
    ));
    let output = assemble_values(
        AssemblyPlan::FillNulls {
            left: &left,
            right: &right,
        },
        &Control,
    )
    .unwrap();
    let list = output.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(list.value_offsets(), &[0, 1, 3, 4]);
    assert_eq!(list.null_count(), 0);
    assert_eq!(
        list.values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some("a"), Some("y"), Some("z"), Some("b")]
    );
}
