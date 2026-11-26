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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::*;
use arrow_schema::{DataType, Field, TimeUnit};
use std::sync::Arc;

fn author(json: bool) -> FunctionValueType {
    let storage = ValueTypeStorageLifetime::pure();
    let item = ValueTypeRef::try_new(
        if json {
            ValueKind::Nominal(ValueLogicalType::Json)
        } else {
            ValueKind::Binary
        },
        true,
        storage.clone(),
    )
    .unwrap();
    let list = ValueTypeRef::try_new(ValueKind::List(item), true, storage.clone()).unwrap();
    let timestamp = DataType::Timestamp(TimeUnit::Nanosecond, Some("Z".repeat(1024).into()));
    let stamp = ValueTypeRef::try_new(ValueKind::Scalar(timestamp.clone()), false, storage.clone())
        .unwrap();
    let name = "界".repeat(300);
    let semantic = ValueTypeRef::try_new(
        ValueKind::Struct(Box::from([
            ValueField::new(name.clone(), list, storage.clone()),
            ValueField::new("stamp", stamp, storage.clone()),
        ])),
        true,
        storage.clone(),
    )
    .unwrap();
    let layout = ValueLayout::try_new(
        &DataType::Struct(
            vec![
                Field::new(
                    name,
                    DataType::List(Arc::new(Field::new(
                        "provider-item",
                        DataType::Dictionary(
                            Box::new(DataType::Int8),
                            Box::new(DataType::LargeBinary),
                        ),
                        true,
                    ))),
                    true,
                ),
                Field::new("stamp", timestamp, false),
            ]
            .into(),
        ),
        storage,
    )
    .unwrap();
    FunctionValueType::try_from_value(semantic, layout).unwrap()
}

#[test]
fn timezone_1024_weighted_bytes_preserve_original_unit_callback_sequence() {
    let make = || {
        FunctionValueType::new(
            DataType::Timestamp(TimeUnit::Microsecond, Some("Z".repeat(1024).into())),
            false,
        )
    };
    let left = make();
    let right = make();
    let mut old = 0;
    assert!(
        left.exactly_equals_observed::<ValueTypeError>(&right, || {
            old += 1;
            Ok(())
        })
        .unwrap()
    );
    let mut steps = Vec::new();
    assert!(
        left.exactly_equals_weighted_observed::<ValueTypeError>(&right, |step| {
            steps.push(step);
            Ok(())
        })
        .unwrap()
    );
    assert_eq!(old, steps.len());
    let bytes = steps
        .iter()
        .filter_map(|step| match step {
            TypeComparisonStep::Bytes { inspected_bytes } => Some(*inspected_bytes),
            _ => None,
        })
        .sum::<usize>();
    assert_eq!(bytes, 4 * 1024); // Semantic scalar and physical layout each inspect both zones.
    assert_eq!(
        steps
            .iter()
            .map(|step| step.work_units_upper())
            .sum::<usize>(),
        left.comparison_work_upper_bound().unwrap()
    );
    for stop in 0..steps.len() {
        let mut seen = Vec::new();
        let result = left.exactly_equals_weighted_observed::<ValueTypeError>(&right, |step| {
            seen.push(step);
            if seen.len() - 1 == stop {
                Err(ValueTypeError::TooManyNodes)
            } else {
                Ok(())
            }
        });
        assert!(matches!(result, Err(ValueTypeError::TooManyNodes)));
        assert_eq!(seen, steps[..=stop]);
    }
}

#[test]
fn independent_equal_and_mismatching_full_bindings_use_frozen_constructor_work() {
    let left = author(true);
    let right = author(true);
    assert!(!left.value_type().shares_storage(right.value_type()));
    assert!(
        !left
            .typed_binding()
            .layout()
            .shares_storage(right.typed_binding().layout())
    );
    let self_work = |source: &FunctionValueType| {
        let mut total = 0usize;
        assert!(
            source
                .exactly_equals_weighted_observed::<ValueTypeError>(source, |step| {
                    total += step.work_units_upper();
                    Ok(())
                })
                .unwrap()
        );
        total
    };
    assert_eq!(
        self_work(&left),
        left.comparison_work_upper_bound().unwrap()
    );
    assert_eq!(
        self_work(&right),
        right.comparison_work_upper_bound().unwrap()
    );
    for other in [right, author(false)] {
        let mut paired = 0usize;
        let equal = left
            .exactly_equals_weighted_observed::<ValueTypeError>(&other, |step| {
                paired += step.work_units_upper();
                Ok(())
            })
            .unwrap();
        assert_eq!(equal, left == other);
        assert!(
            paired
                <= left.comparison_work_upper_bound().unwrap()
                    + other.comparison_work_upper_bound().unwrap()
        );
    }
    let child = left
        .typed_binding()
        .child_at(1, ValueTypeStorageLifetime::pure())
        .unwrap();
    let mut weighted = 0;
    assert!(
        child
            .exactly_equals_weighted_observed::<ValueTypeError>(&child, |step| {
                weighted += step.work_units_upper();
                Ok(())
            })
            .unwrap()
    );
    assert_eq!(weighted, child.comparison_work_upper_bound().unwrap());
    let nullable = left.with_nullable(false).unwrap();
    assert_eq!(
        nullable.comparison_work_upper_bound().unwrap(),
        left.comparison_work_upper_bound().unwrap()
    );
    assert!(
        nullable
            .typed_binding()
            .layout()
            .shares_storage(left.typed_binding().layout())
    );
}
