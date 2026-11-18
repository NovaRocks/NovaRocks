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

//! Exact observed List conversions through LocalCompiler and Frame, with independent nested values.
use super::*;
use arrow::array::{Int32Array, ListArray, NullArray, StringArray, UInt64Array};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::Field;
use novarocks_expression::EvaluationInput;
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};
use std::collections::HashMap;
fn source_type(null_child: bool) -> DataType {
    DataType::List(Arc::new(Field::new(
        "item",
        if null_child {
            DataType::Null
        } else {
            DataType::Utf8
        },
        true,
    )))
}
pub(super) fn target_type(null_child: bool, field_id: Option<&str>) -> DataType {
    let field = Field::new(
        if field_id.is_some() { "element" } else { "" },
        if null_child {
            DataType::Int32
        } else {
            DataType::Utf8
        },
        true,
    );
    let raw = DataType::List(Arc::new(if let Some(id) = field_id {
        field.with_metadata(HashMap::from([("PARQUET:field_id".into(), id.into())]))
    } else {
        field
    }));
    FunctionValueType::new(raw, true).data_type().clone()
}
pub(super) fn input(null_child: bool) -> ArrayRef {
    let DataType::List(field) = source_type(null_child) else {
        unreachable!()
    };
    let values: ArrayRef = if null_child {
        Arc::new(NullArray::new(5))
    } else {
        Arc::new(StringArray::from(vec![
            Some("beijing"),
            Some("shanghai"),
            None,
            Some("字\0x"),
            Some(""),
        ]))
    };
    Arc::new(ListArray::new(
        field,
        OffsetBuffer::new(vec![0_i32, 2, 2, 3, 5].into()),
        values,
        Some(NullBuffer::from(vec![true, true, false, true])),
    ))
}
fn modes() -> [(DecimalOverflowPolicy, bool); 4] {
    [
        (DecimalOverflowPolicy::OutputNull, false),
        (DecimalOverflowPolicy::OutputNull, true),
        (DecimalOverflowPolicy::ReportError, false),
        (DecimalOverflowPolicy::ReportError, true),
    ]
}
fn same_values(left: &ArrayRef, right: &ArrayRef) {
    assert_eq!(left.data_type(), right.data_type());
    assert_eq!(left.len(), right.len());
    for row in 0..left.len() {
        assert_eq!(left.is_null(row), right.is_null(row));
        if !left.is_null(row) {
            let left = left
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value(row);
            let right = right
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value(row);
            assert_eq!(left.to_data(), right.to_data());
        }
    }
}

// Independent nested values: NULL children lift to typed Int32 NULLs; the
// physical Utf8 case only normalizes field annotations and keeps every byte.
fn expected_full(null_child: bool, target: &DataType) -> ArrayRef {
    let DataType::List(field) = target else {
        unreachable!()
    };
    let values: ArrayRef = if null_child {
        Arc::new(Int32Array::from(vec![None::<i32>; 5]))
    } else {
        Arc::new(StringArray::from(vec![
            Some("beijing"),
            Some("shanghai"),
            None,
            Some("字\0x"),
            Some(""),
        ]))
    };
    Arc::new(ListArray::new(
        field.clone(),
        OffsetBuffer::new(vec![0_i32, 2, 2, 3, 5].into()),
        values,
        Some(NullBuffer::from(vec![true, true, false, true])),
    ))
}
fn batch(program: &LocalProgram, source: ArrayRef) -> RecordBatch {
    let len = source.len();
    RecordBatch::try_new(
        program.graph().nodes()[1].output_layout().schema().clone(),
        vec![
            source,
            Arc::new(Int64Array::from(vec![42; len])),
            Arc::new(BooleanArray::from(vec![true; len])),
        ],
    )
    .unwrap()
}
fn compare(null: bool, id: Option<&str>) {
    for (policy, allow) in modes() {
        let full = input(null);
        let target = target_type(null, id);
        let program = compiled(
            FunctionValueType::new(full.data_type().clone(), true),
            FunctionValueType::new(target.clone(), true),
            Source::Column,
            Wrap::Bare,
            policy,
            allow,
        );
        let full_expected = expected_full(null, &target);
        for (source, offset, length) in [
            (full.clone(), 0, 4),
            (full.slice(1, 3), 1, 3),
            (full.slice(1, 0), 1, 0),
        ] {
            let b = batch(&program, source.clone());
            let expected_source = full_expected.slice(offset, length);
            for rows in [
                (0..source.len()).collect::<Vec<_>>(),
                (0..source.len()).filter(|r| r % 2 == 0).collect(),
                Vec::new(),
            ] {
                let selection = Selection::try_sparse(source.len(), &rows).unwrap();
                let out = instance(&program)
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: &b,
                            source: None,
                        },
                        ScalarInvocationScope::new(selection, selection),
                        ScalarInvocationActivation::Activated,
                        &Control,
                        &mut PassThroughCallHost,
                    )
                    .unwrap();
                assert_eq!(out.selection(), selection);
                assert_eq!(out.values().data_type(), &target);
                assert!(out.errors().is_empty());
                let indices = UInt64Array::from(
                    rows.iter()
                        .map(|r| u64::try_from(*r).unwrap())
                        .collect::<Vec<_>>(),
                );
                let expected =
                    arrow::compute::take(expected_source.as_ref(), &indices, None).unwrap();
                same_values(out.values(), &expected);
            }
        }
    }
}
#[test]
fn observed_list_cast_actual_compiler_null_child_to_int32_exact_profile() {
    compare(true, None);
}
#[test]
fn observed_list_cast_actual_compiler_utf8_to_field_id7_exact_profile() {
    compare(false, Some("7"));
}
#[test]
fn observed_list_cast_actual_compiler_utf8_to_field_id6_exact_profile() {
    compare(false, Some("6"));
}
