// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::FunctionValueType;
use crate::{ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef, ValueTypeStorageLifetime};
use arrow_schema::{DataType, Field};
use std::sync::Arc;

fn list(domain: ValueLogicalType, root_nullable: bool, child_nullable: bool) -> FunctionValueType {
    let storage = ValueTypeStorageLifetime::pure();
    let child =
        FunctionValueType::try_with_logical_type(DataType::Utf8, child_nullable, domain).unwrap();
    let value = ValueTypeRef::try_new(
        ValueKind::List(child.value_type().clone()),
        root_nullable,
        storage.clone(),
    )
    .unwrap();
    let layout = ValueLayout::try_new(
        &DataType::List(Arc::new(Field::new("item", DataType::Utf8, child_nullable))),
        storage,
    )
    .unwrap();
    FunctionValueType::try_from_value(value, layout).unwrap()
}

#[test]
fn nested_nominal_identity_survives_normalized_empty_metadata() {
    let json = list(ValueLogicalType::Json, false, false);
    let text = list(ValueLogicalType::Physical, false, false);
    assert_eq!(json.data_type(), text.data_type());
    assert!(!json.same_semantics(&text));
    assert!(!json.same_value_domain(&text));
    assert!(!json.fits_value_type(&text));
    assert_eq!(
        json.child_at(0).unwrap().logical_type(),
        ValueLogicalType::Json
    );
    let DataType::List(item) = json.data_type() else {
        unreachable!()
    };
    assert!(item.metadata().is_empty());
}

#[test]
fn nullable_projection_changes_only_selected_root() {
    let original = list(ValueLogicalType::Json, false, false);
    let widened = original.with_nullable(true).unwrap();
    assert!(!original.nullable());
    assert!(widened.nullable());
    assert!(!widened.value_type().child_type_at(0).unwrap().nullable());
    assert!(original.same_value_domain(&widened));
    assert!(original.fits_value_type(&widened));
    assert!(!widened.fits_value_type(&original));
    assert!(!original.same_value_domain(&list(ValueLogicalType::Json, true, true)));
}

#[test]
fn declared_layout_constructor_does_not_read_nested_annotations() {
    let source = DataType::List(Arc::new(
        Field::new("provider_item", DataType::Utf8, true).with_metadata(
            std::collections::HashMap::<String, String>::from([(
                crate::NR_LOGICAL_TYPE_KEY.into(),
                "unknown-tag".into(),
            )]),
        ),
    ));
    let physical =
        FunctionValueType::try_with_logical_type(source, false, ValueLogicalType::Physical)
            .unwrap();
    assert_eq!(
        physical.child_at(0).unwrap().logical_type(),
        ValueLogicalType::Physical
    );
    let DataType::List(item) = physical.data_type() else {
        unreachable!()
    };
    assert_eq!(item.name(), "item");
    assert!(item.metadata().is_empty());
}

#[test]
fn observed_clone_reports_only_two_handle_operations_and_first_refusal() {
    let value = list(ValueLogicalType::Json, false, true);
    let mut calls = 0;
    let cloned = value
        .clone_observed(|| {
            calls += 1;
            Ok::<(), &'static str>(())
        })
        .unwrap();
    assert_eq!(calls, 2);
    assert_eq!(value, cloned);
    for stop in [1, 2] {
        let mut calls = 0;
        let result = value.clone_observed(|| {
            calls += 1;
            if calls == stop {
                Err("original refusal")
            } else {
                Ok(())
            }
        });
        assert_eq!(result.unwrap_err(), "original refusal");
        assert_eq!(calls, stop);
    }
}

#[test]
fn borrowed_child_comparison_observes_first_refusal_without_projection() {
    let left = list(ValueLogicalType::Json, false, false);
    let right = list(ValueLogicalType::Json, true, true);
    let mut calls = 0;
    let result = FunctionValueType::borrowed_value_domains_equal_observed::<crate::ValueTypeError>(
        left.value_type().child_type_at(0).unwrap(),
        right.value_type().child_type_at(0).unwrap(),
        || {
            calls += 1;
            Err(crate::ValueTypeError::InvalidType("original refusal"))
        },
    );
    assert_eq!(
        result,
        Err(crate::ValueTypeError::InvalidType("original refusal"))
    );
    assert_eq!(calls, 1);
}

#[test]
fn checked_replay_exactness_accepts_callback_only_error_domain() {
    let value = list(ValueLogicalType::Json, true, false);
    let cloned = value.clone();
    assert!(
        value
            .exactly_equals_checked_observed(&cloned, || Ok::<(), &'static str>(()))
            .unwrap()
    );
    assert!(
        !value
            .exactly_equals_checked_observed(
                &list(ValueLogicalType::Physical, true, false),
                || Ok::<(), &'static str>(())
            )
            .unwrap()
    );
    let mut calls = 0;
    let result = value.exactly_equals_checked_observed(&cloned, || {
        calls += 1;
        Err("original replay refusal")
    });
    assert_eq!(result, Err("original replay refusal"));
    assert_eq!(calls, 1);
}

#[test]
fn physical_binding_comparison_ignores_only_root_nullability_and_keeps_original_refusal() {
    let original = list(ValueLogicalType::Json, false, false);
    for (other, expected) in [
        (list(ValueLogicalType::Json, true, false), true),
        (list(ValueLogicalType::Json, false, true), false),
        (list(ValueLogicalType::Physical, false, false), false),
    ] {
        let mut calls = 0;
        assert_eq!(
            original
                .same_physical_binding_ignoring_root_nullability_observed(&other, || {
                    calls += 1;
                    Ok::<(), &'static str>(())
                })
                .unwrap(),
            expected
        );
        for stop in 1..=calls {
            let mut seen = 0;
            let result =
                original.same_physical_binding_ignoring_root_nullability_observed(&other, || {
                    seen += 1;
                    assert!(seen <= stop, "observation after original refusal");
                    if seen == stop {
                        Err("original refusal")
                    } else {
                        Ok(())
                    }
                });
            assert_eq!(result, Err("original refusal"));
            assert_eq!(seen, stop);
        }
    }
    let narrow = list(ValueLogicalType::Physical, false, true);
    let wide = FunctionValueType::new(
        DataType::List(Arc::new(Field::new("item", DataType::LargeUtf8, true))),
        true,
    );
    assert!(narrow.same_value_domain(&wide));
    assert!(
        !narrow
            .same_physical_binding_ignoring_root_nullability_observed(&wide, || Ok::<
                (),
                &'static str,
            >(()))
            .unwrap()
    );
}
