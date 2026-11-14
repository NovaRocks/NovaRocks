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
use crate::FunctionVisibility;
use crate::{EngineFunctionCatalog, EngineFunctionCatalogBuilder};

fn catalog() -> EngineFunctionCatalog {
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(value_conversion_definition().unwrap())
        .unwrap();
    builder.seal().unwrap()
}
fn value(ty: FunctionValueType) -> FunctionArgument {
    FunctionArgument::Value {
        value_type: ty,
        constant: None,
    }
}
fn physical(carrier: DataType, nullable: bool) -> FunctionValueType {
    FunctionValueType::from_legacy_declared_projection(carrier, nullable)
}
fn logical(carrier: DataType, nullable: bool, domain: ValueLogicalType) -> FunctionValueType {
    FunctionValueType::try_with_logical_type(carrier, nullable, domain).unwrap()
}
fn request<'a>(
    args: &'a [FunctionArgument],
    target: &'a FunctionValueType,
) -> FunctionBindingRequest<'a> {
    FunctionBindingRequest {
        arguments: args,
        logical_argument_count: 1,
        expected_result_type: Some(target),
    }
}
fn freeze(source: FunctionValueType, target: &FunctionValueType, overload: &str) {
    let catalog = catalog();
    let args = [value(source)];
    let binding = catalog
        .resolve_bound_trusted(
            VALUE_CONVERSION_NAME,
            FunctionKind::Scalar,
            request(&args, target),
            crate::binding_test_control(),
        )
        .unwrap();
    assert_eq!(binding.function_id.as_str(), VALUE_CONVERSION_FUNCTION_ID);
    assert_eq!(binding.selected.overload.as_str(), overload);
    assert_eq!(
        binding.selected.result_type,
        FunctionResultType::Scalar(target.clone())
    );
    catalog
        .validate_bound(
            &binding,
            request(&args, target),
            crate::binding_test_control(),
        )
        .unwrap();
}

#[test]
fn explicit_opaque_binary_erasure_freezes_the_only_owner_without_inverse_authority() {
    for (domain, carrier) in [
        (ValueLogicalType::Hll, DataType::Binary),
        (ValueLogicalType::Bitmap, DataType::Binary),
        (ValueLogicalType::Object, DataType::Binary),
        (ValueLogicalType::Percentile, DataType::Binary),
        (ValueLogicalType::Variant, DataType::LargeBinary),
    ] {
        let source = logical(carrier.clone(), true, domain);
        let target = physical(carrier.clone(), true);
        freeze(source.clone(), &target, OPAQUE_BINARY);
        assert!(resolve_value_conversion(&target, &source, crate::binding_test_control()).is_err());
        assert!(conversion_intermediate_type(&source, &physical(DataType::Utf8, true)).is_err());
        assert!(
            resolve_value_conversion(
                &source,
                &physical(carrier.clone(), false),
                crate::binding_test_control()
            )
            .is_err()
        );
    }
    let source = logical(DataType::LargeBinary, true, ValueLogicalType::Variant);
    let final_target = physical(DataType::Binary, true);
    let intermediate = conversion_intermediate_type(&source, &final_target)
        .unwrap()
        .unwrap();
    assert_eq!(intermediate.data_type(), source.data_type());
    assert_eq!(intermediate.logical_type(), ValueLogicalType::Physical);
    freeze(source, &intermediate, OPAQUE_BINARY);
    // No UTF8 JSON-validation or fixed-width opaque source is introduced.
    assert!(
        resolve_value_conversion(
            &physical(DataType::Utf8, false),
            &logical(DataType::Utf8, false, ValueLogicalType::Json),
            crate::binding_test_control()
        )
        .is_err()
    );
}

#[test]
fn exact_hidden_owner_and_full_effects_are_declared() {
    let definition = value_conversion_definition().unwrap();
    let declaration = definition.binding_declaration().unwrap();
    declaration.validate_complete_effects().unwrap();
    assert_eq!(
        declaration
            .overloads()
            .iter()
            .map(|overload| overload.identity.as_str())
            .collect::<Vec<_>>(),
        [
            JSON_TEXT,
            LARGEINT_DECIMAL,
            LARGEINT_FLOAT,
            LARGEINT_SIGNED,
            LARGEINT_TEXT,
            NULL_LIFT,
            OPAQUE_BINARY,
            SIGNED_LARGEINT,
            UTF8_LARGEINT
        ]
    );
    for overload in declaration.overloads() {
        assert_eq!(
            overload.effects.as_ref().unwrap(),
            &overload_effects(overload.identity.as_str())
        );
    }
    let mut changed_overloads = declaration.overloads().to_vec();
    changed_overloads[0].result_pattern = "different declared NULL/overflow policy".into();
    let changed = FunctionBindingDeclaration::try_new_complete(
        declaration.function_id().clone(),
        FunctionKind::Scalar,
        changed_overloads,
    )
    .unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(
            FunctionDefinition::try_new_bound(
                VALUE_CONVERSION_NAME,
                FunctionVisibility::Hidden,
                changed,
                Arc::new(ValueConversionResolver),
            )
            .unwrap(),
        )
        .unwrap();
    assert_ne!(catalog().digest(), builder.seal().unwrap().digest());
    let source = [value(logical(DataType::Utf8, true, ValueLogicalType::Json))];
    let target = physical(DataType::Utf8, true);
    assert!(matches!(
        catalog().resolve_bound_user(
            VALUE_CONVERSION_NAME,
            FunctionKind::Scalar,
            request(&source, &target),
            crate::binding_test_control()
        ),
        Err(FunctionBindingError::HiddenFunction)
    ));
}

#[test]
fn actual_supported_root_pairs_resolve_and_freeze() {
    freeze(
        logical(DataType::Utf8, false, ValueLogicalType::Json),
        &physical(DataType::Utf8, true),
        JSON_TEXT,
    );
    for carrier in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ] {
        freeze(
            physical(carrier.clone(), false),
            &logical(
                DataType::FixedSizeBinary(16),
                false,
                ValueLogicalType::LargeInt,
            ),
            SIGNED_LARGEINT,
        );
        freeze(
            logical(
                DataType::FixedSizeBinary(16),
                false,
                ValueLogicalType::LargeInt,
            ),
            &physical(carrier, true),
            LARGEINT_SIGNED,
        );
    }
    for carrier in [DataType::Float32, DataType::Float64] {
        freeze(
            logical(
                DataType::FixedSizeBinary(16),
                false,
                ValueLogicalType::LargeInt,
            ),
            &physical(carrier, false),
            LARGEINT_FLOAT,
        );
    }
}

fn json_field_type(domain: bool, name: &str, nullable: bool) -> Arc<Field> {
    let mut metadata =
        std::collections::HashMap::from([("source".to_string(), "user-column".to_string())]);
    if domain {
        metadata.insert(NR_LOGICAL_TYPE_KEY.to_string(), "json".to_string());
    }
    Arc::new(Field::new(name, DataType::Utf8, nullable).with_metadata(metadata))
}

#[test]
fn nested_same_structure_preserves_every_other_field_fact() {
    let source = physical(
        DataType::Struct(
            vec![Field::new(
                "items",
                DataType::List(json_field_type(true, "item", true)),
                false,
            )]
            .into(),
        ),
        false,
    );
    let target = physical(
        DataType::Struct(
            vec![Field::new(
                "items",
                DataType::List(json_field_type(false, "item", true)),
                false,
            )]
            .into(),
        ),
        false,
    );
    freeze(source.clone(), &target, JSON_TEXT);
    for (child, refused) in [
        (json_field_type(false, "renamed", true), false),
        (json_field_type(false, "item", false), true),
        (Arc::new(Field::new("item", DataType::Utf8, true)), false),
    ] {
        let wrong = physical(
            DataType::Struct(vec![Field::new("items", DataType::List(child), false)].into()),
            false,
        );
        assert_eq!(
            catalog()
                .resolve_bound_trusted(
                    VALUE_CONVERSION_NAME,
                    FunctionKind::Scalar,
                    request(&[value(source.clone())], &wrong),
                    crate::binding_test_control()
                )
                .is_err(),
            refused
        );
    }
}

#[test]
fn invalid_domains_policies_and_missing_explicit_target_are_rejected() {
    let large = logical(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::LargeInt,
    );
    for (source, target) in [
        (
            physical(DataType::FixedSizeBinary(16), false),
            physical(DataType::Int64, true),
        ),
        (
            logical(DataType::FixedSizeBinary(16), false, ValueLogicalType::Uuid),
            physical(DataType::Int64, true),
        ),
        (
            physical(DataType::Utf8, false),
            logical(DataType::Utf8, false, ValueLogicalType::Json),
        ),
        (physical(DataType::UInt64, false), large.clone()),
        (large.clone(), physical(DataType::Int64, false)),
        (large.clone(), physical(DataType::Decimal128(18, 2), false)),
        (
            logical(DataType::Utf8, true, ValueLogicalType::Json),
            physical(DataType::Utf8, false),
        ),
        (
            logical(DataType::Utf8, false, ValueLogicalType::Json),
            physical(DataType::LargeUtf8, false),
        ),
    ] {
        assert!(
            catalog()
                .resolve_bound_trusted(
                    VALUE_CONVERSION_NAME,
                    FunctionKind::Scalar,
                    request(&[value(source)], &target),
                    crate::binding_test_control()
                )
                .is_err()
        );
    }
    let arguments = [value(large)];
    assert!(
        catalog()
            .resolve_bound_trusted(
                VALUE_CONVERSION_NAME,
                FunctionKind::Scalar,
                FunctionBindingRequest {
                    arguments: &arguments,
                    logical_argument_count: 1,
                    expected_result_type: None
                },
                crate::binding_test_control()
            )
            .is_err()
    );
}

#[test]
fn frozen_validation_cannot_reselect_or_replace_full_target() {
    let catalog = catalog();
    let source = [value(logical(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::LargeInt,
    ))];
    let target = physical(DataType::Int64, true);
    let binding = catalog
        .resolve_bound_trusted(
            VALUE_CONVERSION_NAME,
            FunctionKind::Scalar,
            request(&source, &target),
            crate::binding_test_control(),
        )
        .unwrap();
    let mut forged = binding.clone();
    forged.selected.overload = FunctionOverloadId::try_new(LARGEINT_FLOAT).unwrap();
    assert!(
        catalog
            .validate_bound(
                &forged,
                request(&source, &target),
                crate::binding_test_control()
            )
            .is_err()
    );
    let wrong = physical(DataType::Int32, true);
    assert!(
        catalog
            .validate_bound(
                &binding,
                request(&source, &wrong),
                crate::binding_test_control()
            )
            .is_err()
    );
    let uuid = [value(logical(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::Uuid,
    ))];
    assert!(
        catalog
            .validate_bound(
                &binding,
                request(&uuid, &target),
                crate::binding_test_control()
            )
            .is_err()
    );
}

#[test]
fn nested_container_parameters_and_dictionary_identity_are_not_rewritten() {
    let a = json_field_type(true, "item", true);
    let b = json_field_type(false, "item", true);
    for (source, target) in [
        (
            DataType::LargeList(a.clone()),
            DataType::LargeList(b.clone()),
        ),
        (
            DataType::FixedSizeList(a.clone(), 3),
            DataType::FixedSizeList(b.clone(), 3),
        ),
        (
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Int64, false),
                            a.as_ref().clone(),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Int64, false),
                            b.as_ref().clone(),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
        ),
    ] {
        freeze(physical(source, false), &physical(target, false), JSON_TEXT);
    }
    let source = [value(physical(
        DataType::FixedSizeList(a.clone(), 3),
        false,
    ))];
    let wrong_width = physical(DataType::FixedSizeList(b.clone(), 4), false);
    assert!(
        catalog()
            .resolve_bound_trusted(
                VALUE_CONVERSION_NAME,
                FunctionKind::Scalar,
                request(&source, &wrong_width),
                crate::binding_test_control()
            )
            .is_err()
    );

    let dictionary = Field::new(
        "dictionary",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        true,
    );
    let source = [value(physical(
        DataType::Struct(vec![a.as_ref().clone(), dictionary.clone()].into()),
        false,
    ))];
    let target = physical(
        DataType::Struct(vec![b.as_ref().clone(), dictionary.clone()].into()),
        false,
    );
    let catalog = catalog();
    let binding = catalog
        .resolve_bound_trusted(
            VALUE_CONVERSION_NAME,
            FunctionKind::Scalar,
            request(&source, &target),
            crate::binding_test_control(),
        )
        .unwrap();
    let changed_order = physical(
        DataType::Struct(vec![b.as_ref().clone(), dictionary.with_dict_is_ordered(true)].into()),
        false,
    );
    assert!(
        catalog
            .resolve_bound_trusted(
                VALUE_CONVERSION_NAME,
                FunctionKind::Scalar,
                request(&source, &changed_order),
                crate::binding_test_control()
            )
            .is_ok()
    );
    assert_eq!(target, changed_order);
    assert!(
        catalog
            .validate_bound(
                &binding,
                request(&source, &changed_order),
                crate::binding_test_control()
            )
            .is_ok()
    );
    let mut forged = binding.clone();
    forged.selected.result_type = FunctionResultType::Scalar(changed_order);
    assert!(
        catalog
            .validate_bound(
                &forged,
                request(&source, &target),
                crate::binding_test_control()
            )
            .is_ok()
    );
}

#[test]
fn two_stage_json_assignment_preserves_source_fields_before_carrier_cast() {
    let source_field = json_field_type(true, "provider-item", false);
    let source = physical(DataType::List(source_field.clone()), false);
    let target = physical(
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        true,
    );
    let intermediate = conversion_intermediate_type(&source, &target)
        .unwrap()
        .unwrap();
    let DataType::List(field) = intermediate.data_type() else {
        panic!("list");
    };
    assert_eq!(field.name(), "item");
    assert!(!field.is_nullable());
    assert!(field.metadata().is_empty());
    assert!(!field.metadata().contains_key(NR_LOGICAL_TYPE_KEY));
    assert!(!intermediate.nullable());
    freeze(source, &intermediate, JSON_TEXT);
    assert!(novarocks_type_contract::preserves_nested_logical_identity(
        intermediate.data_type(),
        target.data_type()
    ));
    assert!(arrow_cast::can_cast_types(
        intermediate.data_type(),
        target.data_type()
    ));
    assert!(
        conversion_intermediate_type(&intermediate, &target)
            .unwrap()
            .is_none()
    );
    assert!(
        conversion_intermediate_type(
            &physical(DataType::Utf8, true),
            &physical(DataType::Utf8, false)
        )
        .is_err(),
        "the same-domain None branch cannot narrow source nullability"
    );
}

#[test]
fn nonliteral_null_lift_has_an_exact_eager_strict_owner_binding() {
    let source = physical(DataType::Null, true);
    for target in [
        logical(DataType::Utf8, true, ValueLogicalType::Json),
        physical(DataType::List(json_field_type(true, "item", false)), true),
    ] {
        assert_eq!(
            conversion_intermediate_type(&source, &target).unwrap(),
            Some(target.clone())
        );
        freeze(source.clone(), &target, NULL_LIFT);
        assert!(
            resolve_value_conversion(
                &source,
                &target
                    .with_nullable(false)
                    .expect("valid function nullability declaration"),
                crate::binding_test_control()
            )
            .is_err()
        );
    }
    assert!(
        conversion_intermediate_type(
            &physical(DataType::Null, false),
            &physical(DataType::Int64, true)
        )
        .is_err()
    );
}

mod exact;

mod largeint_datetime;
