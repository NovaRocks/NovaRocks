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

use super::*;
use crate::{ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime};
use arrow_schema::Field;
use std::sync::Arc;

fn item(json: bool, nullable: bool) -> ValueTypeRef {
    ValueTypeRef::try_new(
        if json {
            ValueKind::Nominal(ValueLogicalType::Json)
        } else {
            ValueKind::String
        },
        nullable,
        ValueTypeStorageLifetime::pure(),
    )
    .unwrap()
}
fn array(value: ValueTypeRef, layout: DataType, nullable: bool) -> FunctionValueType {
    let storage = ValueTypeStorageLifetime::pure();
    let layout = ValueLayout::try_new(
        &DataType::List(Arc::new(Field::new(
            "declared_item",
            layout,
            value.nullable(),
        ))),
        storage.clone(),
    )
    .unwrap();
    FunctionValueType::try_from_value(
        ValueTypeRef::try_new(ValueKind::List(value), nullable, storage).unwrap(),
        layout,
    )
    .unwrap()
}
fn list(json: bool, child_nullable: bool, root_nullable: bool) -> FunctionValueType {
    let carrier = if json {
        DataType::LargeBinary
    } else {
        DataType::Utf8
    };
    array(item(json, child_nullable), carrier, root_nullable)
}
fn check(
    source: &FunctionValueType,
    target: &FunctionValueType,
    mode: ArrayCastFailureMode,
) -> bool {
    array_cast_mode_types_valid_observed::<std::convert::Infallible>(
        source,
        target,
        mode,
        || Ok(()),
    )
    .unwrap()
}

#[test]
fn json_null_requires_nullable_text_even_when_json_source_is_required() {
    // JSON's non-SQL-NULL text "null" is a successful SQL NULL after casting.
    let json = list(true, false, true);
    let required_text = list(false, false, true);
    let optional_text = list(false, true, true);
    assert!(
        !crate::preserves_value_domains_observed::<std::convert::Infallible>(
            json.value_type(),
            optional_text.value_type(),
            |_| Ok(())
        )
        .unwrap()
    );
    for mode in [
        ArrayCastFailureMode::ReportError,
        ArrayCastFailureMode::OutputNull,
    ] {
        assert!(!check(&json, &required_text, mode));
        assert!(check(&json, &optional_text, mode));
        assert!(check(&json, &json, mode));
    }
}

#[test]
fn required_string_can_encode_required_json_but_nullable_string_cannot() {
    // A non-NULL VARCHAR "null" encodes a JSON string, not a JSON null.
    let required_text = list(false, false, true);
    let optional_text = list(false, true, true);
    let required_json = list(true, false, true);
    assert!(
        !crate::preserves_value_domains_observed::<std::convert::Infallible>(
            required_text.value_type(),
            required_json.value_type(),
            |_| Ok(())
        )
        .unwrap()
    );
    for mode in [
        ArrayCastFailureMode::ReportError,
        ArrayCastFailureMode::OutputNull,
    ] {
        assert!(check(&required_text, &required_json, mode));
        assert!(!check(&optional_text, &required_json, mode));
    }
}

#[test]
fn nested_json_domain_guard_is_recursive_and_try_needs_nullable_root() {
    let storage = ValueTypeStorageLifetime::pure();
    let json_inner =
        ValueTypeRef::try_new(ValueKind::List(item(true, false)), false, storage.clone()).unwrap();
    let text_inner =
        ValueTypeRef::try_new(ValueKind::List(item(false, false)), false, storage).unwrap();
    let json_layout = DataType::List(Arc::new(Field::new("inner", DataType::LargeBinary, false)));
    let text_layout = DataType::List(Arc::new(Field::new("inner", DataType::Utf8, false)));
    let json = array(json_inner, json_layout, true);
    let text = array(text_inner, text_layout, true);
    assert!(!check(&json, &text, ArrayCastFailureMode::ReportError));
    let storage = ValueTypeStorageLifetime::pure();
    let nullable_text_inner =
        ValueTypeRef::try_new(ValueKind::List(item(false, true)), false, storage).unwrap();
    let nullable_text_layout = DataType::List(Arc::new(Field::new("inner", DataType::Utf8, true)));
    let nullable_text = array(nullable_text_inner, nullable_text_layout, true);
    assert!(check(
        &json,
        &nullable_text,
        ArrayCastFailureMode::ReportError
    ));
    assert!(
        !crate::preserves_value_domains_observed::<std::convert::Infallible>(
            json.value_type(),
            nullable_text.value_type(),
            |_| Ok(())
        )
        .unwrap()
    );

    let json = list(true, false, false);
    let text = list(false, true, false);
    assert!(check(&json, &text, ArrayCastFailureMode::ReportError));
    assert!(!check(&json, &text, ArrayCastFailureMode::OutputNull));
}

#[test]
fn domain_guard_preserves_each_original_observation_refusal() {
    let source = list(true, false, true);
    let target = list(false, true, true);
    let mut total = 0_usize;
    assert!(
        array_cast_mode_types_valid_observed(
            &source,
            &target,
            ArrayCastFailureMode::ReportError,
            || {
                total += 1;
                Ok::<_, usize>(())
            }
        )
        .unwrap()
    );
    assert!(total > 0);
    #[derive(Debug)]
    struct Refusal {
        token: u64,
    }
    let refusal = Arc::new(Refusal { token: 731 });
    for stop in 1..=total {
        let mut seen = 0;
        let error = array_cast_mode_types_valid_observed(
            &source,
            &target,
            ArrayCastFailureMode::ReportError,
            || {
                seen += 1;
                assert!(seen <= stop, "observation after refusal");
                if seen == stop {
                    Err(Arc::clone(&refusal))
                } else {
                    Ok(())
                }
            },
        )
        .unwrap_err();
        assert!(Arc::ptr_eq(&error, &refusal));
        assert_eq!(error.token, 731);
        assert_eq!(seen, stop);
    }
}

#[test]
fn explicit_domain_exceptions_do_not_grant_non_array_mode() {
    let json_scalar = FunctionValueType::try_from_value(
        item(true, false),
        ValueLayout::try_new(&DataType::LargeBinary, ValueTypeStorageLifetime::pure()).unwrap(),
    )
    .unwrap();
    let text_scalar = FunctionValueType::new(DataType::Utf8, true);
    let json_list = list(true, false, true);
    for mode in [
        ArrayCastFailureMode::ReportError,
        ArrayCastFailureMode::OutputNull,
    ] {
        assert!(!check(&json_scalar, &text_scalar, mode));
        assert!(!check(&json_list, &text_scalar, mode));
        assert!(!check(&text_scalar, &json_list, mode));
    }
}
