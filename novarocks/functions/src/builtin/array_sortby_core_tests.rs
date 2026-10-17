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
use arrow_array::{Int32Array, Int64Array, StringArray, types::Int64Type};
use arrow_buffer::{NullBuffer, ScalarBuffer};
use arrow_schema::Field;

fn ints(rows: Vec<Option<Vec<Option<i64>>>>) -> ArrayRef {
    Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(rows))
}
fn one(values: &[i64]) -> ArrayRef {
    ints(vec![Some(values.iter().copied().map(Some).collect())])
}
fn text(values: &[&str]) -> ArrayRef {
    let values: ArrayRef = Arc::new(StringArray::from(values.to_vec()));
    Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, values.len() as i32])),
        values,
        None,
    ))
}
fn output(source: &ArrayRef, keys: &[ArrayRef]) -> Result<ArrayRef, String> {
    evaluate(
        source,
        keys.len(),
        |i| Ok(keys[i].clone()),
        None,
        source.len(),
    )
}
fn values(output: &ArrayRef) -> Vec<Option<i64>> {
    let list = output.as_any().downcast_ref::<ListArray>().unwrap();
    let array = list.value(0);
    array
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .collect()
}
#[test]
fn sortby_preserves_one_through_six_independent_key_types_and_stable_ties() {
    let source = one(&[10, 20, 30, 40]);
    for count in 1..=6 {
        let mut keys = vec![one(&[1, 0, 1, 0])];
        for _ in 1..count {
            keys.push(text(&["same", "same", "same", "same"]));
        }
        assert_eq!(
            values(&output(&source, &keys).unwrap()),
            vec![Some(20), Some(40), Some(10), Some(30)]
        );
    }
    // Every supported key position must actually decide a previous tie.
    for count in 1..=6 {
        let mut keys = (0..count - 1)
            .map(|index| {
                if index % 2 == 0 {
                    one(&[0, 0, 0, 0])
                } else {
                    text(&["x", "x", "x", "x"])
                }
            })
            .collect::<Vec<_>>();
        keys.push(if count % 2 == 0 {
            text(&["b", "d", "a", "c"])
        } else {
            one(&[1, 3, 0, 2])
        });
        assert_eq!(
            values(&output(&source, &keys).unwrap()),
            vec![Some(30), Some(10), Some(40), Some(20)]
        );
    }
    let keys = [one(&[1, 0, 1, 0]), text(&["z", "z", "a", "a"])];
    assert_eq!(
        values(&output(&source, &keys).unwrap()),
        vec![Some(40), Some(20), Some(30), Some(10)]
    );
}
#[test]
fn sortby_null_elements_first_and_whole_null_key_ignored() {
    let source = one(&[10, 20, 30, 40]);
    let element_null = ints(vec![Some(vec![Some(2), None, Some(2), Some(1)])]);
    assert_eq!(
        values(&output(&source, &[element_null]).unwrap()),
        vec![Some(20), Some(40), Some(10), Some(30)]
    );
    let nullable_values = ints(vec![Some(vec![Some(10), None, Some(30)])]);
    assert_eq!(
        values(&output(&nullable_values, &[one(&[2, 1, 3])]).unwrap()),
        vec![None, Some(10), Some(30)]
    );
    let ignored: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Int64, true)),
        OffsetBuffer::new(vec![0_i32, 0].into()),
        Arc::new(Int64Array::from(Vec::<i64>::new())),
        Some(NullBuffer::new_null(1)),
    ));
    assert_eq!(
        values(&output(&source, &[ignored]).unwrap()),
        vec![Some(10), Some(20), Some(30), Some(40)]
    );
}
#[test]
fn sortby_original_nan_comparison_is_equal_without_total_order_replacement() {
    let source = one(&[10, 20, 30]);
    let values: ArrayRef = Arc::new(arrow_array::Float64Array::from(vec![2.0, f64::NAN, 1.0]));
    let key: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Float64, true)),
        OffsetBuffer::new(vec![0_i32, 3].into()),
        values,
        None,
    ));
    assert_eq!(
        self::values(&output(&source, &[key]).unwrap()),
        vec![Some(10), Some(20), Some(30)]
    );
}
#[test]
fn sortby_source_null_skips_key_lengths_but_nonnull_mismatch_keeps_full_error() {
    let source = ints(vec![None]);
    let result = output(&source, &[one(&[1, 2])]).unwrap();
    assert!(result.is_null(0));
    assert_eq!(
        output(&one(&[10, 20]), &[one(&[1])]).unwrap_err(),
        "Expr evaluate meet error: Input arrays' size are not equal in array_sortby."
    );
}
#[test]
fn sortby_original_conditional_cast_and_exact_target_field_remain() {
    let source = one(&[20, 10]);
    let field = Arc::new(Field::new("original-target", DataType::Int32, true));
    let target = DataType::List(field.clone());
    let result = evaluate(&source, 1, |_| Ok(one(&[2, 1])), Some(&target), 1).unwrap();
    assert_eq!(result.data_type(), &target);
    let list = result.as_any().downcast_ref::<ListArray>().unwrap();
    let values = list.value(0);
    assert_eq!(
        values
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[10, 20]
    );
}
#[test]
fn sortby_source_shape_refusal_precedes_original_lazy_key_evaluation() {
    let source: ArrayRef = Arc::new(StringArray::from(vec!["bad-source"]));
    let mut keys = 0;
    let result = evaluate(
        &source,
        1,
        |_| {
            keys += 1;
            Err("later key failure".into())
        },
        None,
        1,
    );
    assert_eq!(keys, 0);
    assert_eq!(
        result.unwrap_err(),
        "array_sortby expects ListArray src, got Utf8"
    );
}

#[test]
fn sortby_lazy_key_failure_preserves_typed_source_and_stops_later_keys() {
    #[derive(Debug, PartialEq)]
    enum Failure {
        Math(String),
        Runtime { category: u8, context: &'static str },
    }
    impl From<String> for Failure {
        fn from(value: String) -> Self {
            Self::Math(value)
        }
    }
    let source = one(&[20, 10]);
    for category in 0..8 {
        let original = Failure::Runtime {
            category,
            context: "actual operator",
        };
        let mut calls = Vec::new();
        let error = evaluate_fallible(
            &source,
            3,
            |ordinal| {
                calls.push(ordinal);
                match ordinal {
                    0 => Ok(one(&[1, 2])),
                    1 => Err(Failure::Runtime {
                        category,
                        context: "actual operator",
                    }),
                    _ => panic!("key evaluation after the first failure"),
                }
            },
            None,
            1,
        )
        .unwrap_err();
        assert_eq!(error, original);
        assert_eq!(calls, vec![0, 1]);
    }
}
