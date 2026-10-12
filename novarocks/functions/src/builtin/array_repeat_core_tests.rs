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
use super::{cast_repeat_count, repeat_values};
use arrow_array::types::Int8Type;
use arrow_array::{
    Array, ArrayRef, DictionaryArray, Float64Array, Int32Array, Int64Array, IntervalDayTimeArray,
    IntervalYearMonthArray, ListArray, StringArray, StructArray, new_empty_array, new_null_array,
};
use arrow_schema::{DataType, Field};
use std::sync::Arc;

#[test]
fn int64_count_keeps_the_input_identity() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![Some(2), None, Some(-1)]));
    let out = cast_repeat_count(source.clone()).unwrap();
    assert!(Arc::ptr_eq(&source, &out));
}

#[test]
fn count_safe_cast_preserves_invalid_null_and_float_truncation() {
    let text: ArrayRef = Arc::new(StringArray::from(vec![
        Some("2"),
        Some("invalid"),
        None,
        Some("-1"),
    ]));
    let out = cast_repeat_count(text).unwrap();
    assert_eq!(
        out.as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(2), None, None, Some(-1)]
    );
    let floats: ArrayRef = Arc::new(Float64Array::from(vec![2.9, f64::NAN, f64::INFINITY, -1.9]));
    let out = cast_repeat_count(floats).unwrap();
    assert_eq!(
        out.as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(2), None, None, Some(-1)]
    );
}

#[test]
fn interval_count_keeps_the_original_full_cast_error() {
    let year_month: ArrayRef = Arc::new(IntervalYearMonthArray::from(vec![1]));
    assert_eq!(
        cast_repeat_count(year_month).unwrap_err(),
        "array_repeat failed to cast repeat count Interval(YearMonth) -> BIGINT: Cast error: Casting from Interval(YearMonth) to Int64 not supported"
    );
    let day_time: ArrayRef = Arc::new(IntervalDayTimeArray::from(vec![
        arrow_buffer::IntervalDayTime::new(1, 0),
    ]));
    assert_eq!(
        cast_repeat_count(day_time).unwrap_err(),
        "array_repeat failed to cast repeat count Interval(DayTime) -> BIGINT: Cast error: Casting from Interval(DayTime) to Int64 not supported"
    );
}

#[test]
fn complete_output_field_and_typed_null_source_are_preserved() {
    let field = Arc::new(
        Field::new("authored", DataType::Int64, true).with_metadata(
            [
                ("owner".into(), "source".into()),
                ("contract".into(), "exact".into()),
            ]
            .into(),
        ),
    );
    let source: ArrayRef = Arc::new(Int32Array::from(vec![Some(7), None]));
    let counts = Int64Array::from(vec![2, 1]);
    let out = repeat_values(source, &counts, 2, field.clone()).unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    match list.data_type() {
        DataType::List(actual) => assert!(Arc::ptr_eq(actual, &field)),
        other => panic!("unexpected output type {other:?}"),
    }
    assert_eq!(list.value_offsets(), &[0, 2, 3]);
    assert_eq!(
        list.values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(7), Some(7), None]
    );
    let out = repeat_values(new_null_array(&DataType::Null, 2), &counts, 2, field).unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(list.null_count(), 0);
    assert_eq!(list.values().data_type(), &DataType::Int64);
    assert_eq!(list.values().null_count(), 3);
}

#[test]
fn materialized_rows_keep_null_nonpositive_and_empty_source_behavior() {
    let field = Arc::new(Field::new("item", DataType::Int32, true));
    let source: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(1),
        None,
        Some(3),
        Some(4),
        Some(5),
    ]));
    let counts = Int64Array::from(vec![Some(2), Some(1), Some(0), Some(-1), None]);
    let out = repeat_values(source, &counts, 5, field.clone()).unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(list.value_offsets(), &[0, 2, 3, 3, 3, 3]);
    assert!(list.is_null(4));
    assert_eq!(list.null_count(), 1);
    assert_eq!(
        list.values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(1), Some(1), None]
    );
    let out = repeat_values(
        new_empty_array(&DataType::Int32),
        &Int64Array::from(vec![3]),
        1,
        field.clone(),
    )
    .unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(list.value_offsets(), &[0, 3]);
    assert_eq!(list.null_count(), 0);
    assert_eq!(list.values().null_count(), 3);
    let out = repeat_values(
        new_empty_array(&DataType::Int32),
        &Int64Array::from(Vec::<i64>::new()),
        0,
        field,
    )
    .unwrap();
    assert_eq!(
        out.as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value_offsets(),
        &[0]
    );
}

#[test]
fn source_cast_keeps_the_original_arrow_error_text() {
    let source: ArrayRef = Arc::new(StructArray::new(
        vec![Arc::new(Field::new("x", DataType::Int32, false))].into(),
        vec![Arc::new(Int32Array::from(vec![1]))],
        None,
    ));
    let expected = format!(
        "array_repeat failed to cast source type {:?} -> {:?}: {}",
        source.data_type(),
        DataType::Int64,
        arrow_cast::cast(&source, &DataType::Int64).unwrap_err()
    );
    let field = Arc::new(Field::new("item", DataType::Int64, true));
    assert_eq!(
        repeat_values(source, &Int64Array::from(vec![1]), 1, field).unwrap_err(),
        expected
    );
}

#[test]
fn dictionary_backing_keeps_the_original_constructor_boundary() {
    for backing_len in [127, 128] {
        let values = StringArray::from_iter_values((0..backing_len).map(|n| format!("value-{n}")));
        let source: ArrayRef = Arc::new(
            DictionaryArray::<Int8Type>::try_new(vec![0_i8].into(), Arc::new(values)).unwrap(),
        );
        let field = Arc::new(Field::new("item", source.data_type().clone(), true));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            repeat_values(source, &Int64Array::from(vec![0]), 1, field)
        }));
        if backing_len == 127 {
            let out = result.unwrap().unwrap();
            let list = out.as_any().downcast_ref::<ListArray>().unwrap();
            assert_eq!(list.value_offsets(), &[0, 0]);
            assert_eq!(
                list.values()
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int8Type>>()
                    .unwrap()
                    .values()
                    .len(),
                127
            );
        } else {
            let panic = result.unwrap_err();
            let message = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .expect("original string panic");
            assert_eq!(
                message,
                "MutableArrayData::new is infallible: DictionaryKeyOverflowError"
            );
        }
    }
}
