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

//! Independent full-value-domain expectations for the actual builtin owner.

use super::*;
use arrow_schema::Field;

fn value(ty: FunctionValueType) -> FunctionArgument {
    FunctionArgument::Value {
        value_type: ty,
        constant: None,
    }
}
fn physical(carrier: DataType) -> FunctionValueType {
    FunctionValueType::from_legacy_declared_projection(carrier, true)
}
fn logical(carrier: DataType, domain: ValueLogicalType) -> FunctionValueType {
    FunctionValueType::try_with_logical_type(carrier, true, domain).unwrap()
}
fn request(arguments: &[FunctionArgument]) -> FunctionBindingRequest<'_> {
    FunctionBindingRequest {
        arguments,
        logical_argument_count: arguments.len(),
        expected_result_type: None,
    }
}
fn resolve(
    name: &str,
    kind: FunctionKind,
    arguments: &[FunctionArgument],
) -> ResolvedFunctionBinding {
    let catalog = builtin_engine_function_catalog();
    let binding = catalog
        .resolve_bound_user(
            name,
            kind,
            request(arguments),
            crate::binding_test_control(),
        )
        .unwrap();
    catalog
        .validate_bound(&binding, request(arguments), crate::binding_test_control())
        .unwrap();
    binding
}
fn output(binding: &ResolvedFunctionBinding) -> &FunctionValueType {
    let FunctionResultType::Scalar(output) = &binding.selected.result_type else {
        panic!("scalar result");
    };
    output
}
fn json() -> FunctionValueType {
    logical(DataType::Utf8, ValueLogicalType::Json)
}

#[test]
fn variant_typeof_keeps_exact_json_and_variant_domains_for_fresh_and_frozen_binding() {
    for nullable in [false, true] {
        for (carrier, domain) in [
            (DataType::Utf8, ValueLogicalType::Json),
            (DataType::LargeBinary, ValueLogicalType::Variant),
        ] {
            let ty = FunctionValueType::try_with_logical_type(carrier, nullable, domain).unwrap();
            let arguments = [value(ty.clone())];
            let binding = resolve("variant_typeof", FunctionKind::Scalar, &arguments);
            assert_eq!(
                binding.selected.argument_types.as_ref(),
                &[FunctionArgumentType::Value(ty)]
            );
            assert_eq!(output(&binding), &physical(DataType::Utf8));
        }
    }
}

#[test]
fn variant_typeof_rejects_plain_carriers_and_cannot_thaw_json_as_text() {
    let catalog = builtin_engine_function_catalog();
    let original = [value(json())];
    let binding = resolve("variant_typeof", FunctionKind::Scalar, &original);
    for carrier in [
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Binary,
        DataType::LargeBinary,
        DataType::Null,
        DataType::Int64,
    ] {
        let arguments = [value(physical(carrier))];
        assert!(
            catalog
                .resolve_bound_user(
                    "variant_typeof",
                    FunctionKind::Scalar,
                    request(&arguments),
                    crate::binding_test_control()
                )
                .is_err()
        );
        assert!(
            catalog
                .validate_bound(&binding, request(&arguments), crate::binding_test_control())
                .is_err()
        );
    }
}
fn json_item(nullable: bool) -> Arc<Field> {
    Arc::new(
        Field::new("item", DataType::Utf8, nullable).with_metadata(
            [(
                novarocks_type_contract::NR_LOGICAL_TYPE_KEY.to_string(),
                "json".to_string(),
            )]
            .into(),
        ),
    )
}

#[test]
fn repeated_variables_select_owner_authorized_common_domains() {
    let catalog = builtin_engine_function_catalog();
    for arguments in [
        vec![value(json()), value(physical(DataType::Utf8))],
        vec![
            value(physical(DataType::List(json_item(true)))),
            value(physical(DataType::List(Arc::new(Field::new(
                "item",
                DataType::Utf8,
                true,
            ))))),
        ],
        vec![
            value(logical(
                DataType::FixedSizeBinary(16),
                ValueLogicalType::LargeInt,
            )),
            value(physical(DataType::Int32)),
        ],
    ] {
        for name in ["coalesce", "ifnull"] {
            let binding = resolve_with_materialized_assignments(name, &arguments);
            assert!(
                catalog
                    .validate_bound(&binding, request(&arguments), crate::binding_test_control())
                    .is_err(),
                "the frozen consumer must see the converted full argument domains"
            );
        }
    }
    let arguments = [value(json()), value(json())];
    let binding = resolve("coalesce", FunctionKind::Scalar, &arguments);
    assert_eq!(output(&binding).logical_type(), ValueLogicalType::Json);
    let mut forged = binding;
    forged.selected.result_type = FunctionResultType::Scalar(physical(DataType::Utf8));
    assert!(
        catalog
            .validate_bound(&forged, request(&arguments), crate::binding_test_control())
            .is_err()
    );
}

fn resolve_with_materialized_assignments(
    name: &str,
    arguments: &[FunctionArgument],
) -> ResolvedFunctionBinding {
    use crate::builtin::value_conversion::{VALUE_CONVERSION_NAME, conversion_intermediate_type};
    let catalog = builtin_engine_function_catalog();
    let binding = catalog
        .resolve_bound_user(
            name,
            FunctionKind::Scalar,
            request(arguments),
            crate::binding_test_control(),
        )
        .unwrap();
    let mut materialized = Vec::new();
    let mut conversions = 0;
    for (source, target) in arguments.iter().zip(&binding.selected.argument_types) {
        let (
            FunctionArgument::Value {
                value_type: source_type,
                ..
            },
            FunctionArgumentType::Value(target_type),
        ) = (source, target)
        else {
            panic!("value arguments");
        };
        if let Some(intermediate) = conversion_intermediate_type(source_type, target_type).unwrap()
        {
            let source = [source.clone()];
            let conversion_request = FunctionBindingRequest {
                arguments: &source,
                logical_argument_count: 1,
                expected_result_type: Some(&intermediate),
            };
            let conversion = catalog
                .resolve_bound_trusted(
                    VALUE_CONVERSION_NAME,
                    FunctionKind::Scalar,
                    conversion_request,
                    crate::binding_test_control(),
                )
                .unwrap();
            catalog
                .validate_bound(
                    &conversion,
                    conversion_request,
                    crate::binding_test_control(),
                )
                .unwrap();
            assert_eq!(output(&conversion), &intermediate);
            conversions += 1;
        }
        materialized.push(value(target_type.clone()));
    }
    assert!(
        conversions > 0,
        "the domain change must have a real owner binding"
    );
    catalog
        .validate_bound(
            &binding,
            request(&materialized),
            crate::binding_test_control(),
        )
        .unwrap();
    binding
}

#[test]
fn string_anchor_and_largeint_anchor_have_actual_conversion_bindings() {
    let binding = resolve_with_materialized_assignments("upper", &[value(json())]);
    assert_eq!(
        binding.selected.argument_types[0],
        FunctionArgumentType::Value(physical(DataType::Utf8))
    );
    assert_eq!(output(&binding).logical_type(), ValueLogicalType::Physical);
    let binding = resolve_with_materialized_assignments(
        "bitand",
        &[
            value(logical(
                DataType::FixedSizeBinary(16),
                ValueLogicalType::LargeInt,
            )),
            value(physical(DataType::Int64)),
        ],
    );
    assert_eq!(output(&binding).logical_type(), ValueLogicalType::LargeInt);
    assert!(binding.selected.argument_types.iter().all(|argument| matches!(argument, FunctionArgumentType::Value(ty) if ty.logical_type() == ValueLogicalType::LargeInt)));
}

#[test]
fn undeclared_domains_do_not_gain_conversion_authority_from_carriers() {
    let catalog = builtin_engine_function_catalog();
    for other in [
        physical(DataType::FixedSizeBinary(16)),
        logical(DataType::FixedSizeBinary(16), ValueLogicalType::Uuid),
    ] {
        let arguments = [
            value(logical(
                DataType::FixedSizeBinary(16),
                ValueLogicalType::LargeInt,
            )),
            value(other),
        ];
        for name in ["coalesce", "ifnull", "bitand"] {
            assert!(
                catalog
                    .resolve_bound_user(
                        name,
                        FunctionKind::Scalar,
                        request(&arguments),
                        crate::binding_test_control()
                    )
                    .is_err()
            );
        }
    }
    let arguments = [
        value(logical(DataType::LargeBinary, ValueLogicalType::Variant)),
        value(physical(DataType::LargeBinary)),
    ];
    assert!(
        catalog
            .resolve_bound_user(
                "coalesce",
                FunctionKind::Scalar,
                request(&arguments),
                crate::binding_test_control()
            )
            .is_err()
    );
}

#[test]
fn nested_common_domain_and_null_variable_targets_keep_exact_conversion_recipes() {
    let mut metadata = std::collections::HashMap::from([
        (
            novarocks_type_contract::NR_LOGICAL_TYPE_KEY.to_string(),
            "json".to_string(),
        ),
        ("provider-field".to_string(), "source-key".to_string()),
    ]);
    let source = FunctionValueType::from_legacy_declared_projection(
        DataType::List(Arc::new(
            Field::new("source_item", DataType::Utf8, false).with_metadata(metadata.clone()),
        )),
        false,
    );
    metadata.remove(novarocks_type_contract::NR_LOGICAL_TYPE_KEY);
    let other = physical(DataType::List(Arc::new(Field::new(
        "item",
        DataType::Utf8,
        true,
    ))));
    let binding =
        resolve_with_materialized_assignments("coalesce", &[value(source.clone()), value(other)]);
    let FunctionArgumentType::Value(target) = &binding.selected.argument_types[0] else {
        panic!("value");
    };
    let intermediate =
        crate::builtin::value_conversion::conversion_intermediate_type(&source, target)
            .unwrap()
            .unwrap();
    let DataType::List(child) = intermediate.data_type() else {
        panic!("list");
    };
    let DataType::List(source_child) = source.data_type() else {
        panic!("source list")
    };
    assert_eq!(child.name(), source_child.name());
    assert!(!child.is_nullable());
    assert!(child.metadata().is_empty());
    let nullable_json = json();
    let null = physical(DataType::Null);
    let binding =
        resolve_with_materialized_assignments("coalesce", &[value(null), value(nullable_json)]);
    assert_eq!(output(&binding).logical_type(), ValueLogicalType::Json);
    assert!(binding.selected.argument_types.iter().all(|argument| matches!(argument, FunctionArgumentType::Value(value) if value.logical_type() == ValueLogicalType::Json)));
}

#[test]
fn actual_ordinary_integer_widening_keeps_physical_targets() {
    let arguments = [
        value(physical(DataType::Utf8)),
        value(physical(DataType::Int16)),
    ];
    let binding = builtin_engine_function_catalog()
        .resolve_bound_user(
            "left",
            FunctionKind::Scalar,
            request(&arguments),
            crate::binding_test_control(),
        )
        .unwrap();
    assert_eq!(
        binding.selected.argument_types[1],
        FunctionArgumentType::Value(physical(DataType::Int64))
    );
    let materialized = [arguments[0].clone(), value(physical(DataType::Int64))];
    builtin_engine_function_catalog()
        .validate_bound(
            &binding,
            request(&materialized),
            crate::binding_test_control(),
        )
        .unwrap();
}

#[test]
fn largeint_anchor_has_exact_domain_and_numeric_results_are_owner_declared() {
    let catalog = builtin_engine_function_catalog();
    for source in [
        physical(DataType::FixedSizeBinary(16)),
        logical(DataType::FixedSizeBinary(16), ValueLogicalType::Uuid),
    ] {
        let arguments = [value(source)];
        assert!(
            catalog
                .resolve_bound_user(
                    "abs",
                    FunctionKind::Scalar,
                    request(&arguments),
                    crate::binding_test_control()
                )
                .is_err()
        );
        assert!(
            catalog
                .resolve_bound_user(
                    "sum",
                    FunctionKind::Aggregate,
                    request(&arguments),
                    crate::binding_test_control()
                )
                .is_err()
        );
    }
    let arguments = [value(logical(
        DataType::FixedSizeBinary(16),
        ValueLogicalType::LargeInt,
    ))];
    for (name, kind) in [
        ("abs", FunctionKind::Scalar),
        ("sum", FunctionKind::Aggregate),
        ("multi_distinct_sum", FunctionKind::Aggregate),
    ] {
        let binding = resolve(name, kind, &arguments);
        assert_eq!(output(&binding).logical_type(), ValueLogicalType::LargeInt);
        if name == "sum" {
            // The exact LARGEINT SUM state is a physical 76-digit decimal.
            assert_eq!(
                binding.selected.aggregate.unwrap().intermediate_type,
                FunctionValueType::new(DataType::Decimal256(76, 0), true)
            );
        } else if name == "multi_distinct_sum" {
            assert_eq!(
                binding.selected.aggregate.unwrap().intermediate_type,
                physical(DataType::Binary)
            );
        }
    }
    let abs_integer = resolve(
        "abs",
        FunctionKind::Scalar,
        &[value(physical(DataType::Int64))],
    );
    assert_eq!(
        output(&abs_integer).logical_type(),
        ValueLogicalType::LargeInt
    );
}

#[test]
fn json_producer_domains_follow_exact_registered_owners() {
    let catalog = builtin_engine_function_catalog();
    for (name, arguments) in [
        ("parse_json", vec![value(physical(DataType::Utf8))]),
        (
            "json_object",
            vec![
                value(physical(DataType::Utf8)),
                value(physical(DataType::Int64)),
            ],
        ),
        (
            "json_query",
            vec![value(json()), value(physical(DataType::Utf8))],
        ),
    ] {
        let binding = resolve(name, FunctionKind::Scalar, &arguments);
        assert_eq!(
            output(&binding).logical_type(),
            ValueLogicalType::Json,
            "{name}"
        );
        assert_eq!(output(&binding).data_type().clone(), DataType::Utf8);
        let definition = catalog.definition_by_id(&binding.function_id).unwrap();
        assert!(
            definition
                .binding_declaration()
                .unwrap()
                .overloads()
                .iter()
                .any(|overload| overload.identity == binding.selected.overload
                    && overload.result_pattern.ends_with(";root=json"))
        );
        let mut forged = binding;
        forged.selected.result_type = FunctionResultType::Scalar(physical(DataType::Utf8));
        assert!(
            catalog
                .validate_bound(&forged, request(&arguments), crate::binding_test_control())
                .is_err(),
            "{name}"
        );
    }
    // A provenance vocabulary entry does not install an implementation.
    // Both declarations are still explicitly unavailable in the manifest.
    for name in ["json_array", "to_json"] {
        assert_eq!(
            super::builtin_disposition(name),
            Some(super::BuiltinDisposition::Unavailable)
        );
        assert!(catalog.definition(name, FunctionKind::Scalar).is_none());
        let arguments = [value(json())];
        assert!(matches!(
            catalog.resolve_bound_user(
                name,
                FunctionKind::Scalar,
                request(&arguments),
                crate::binding_test_control()
            ),
            Err(FunctionBindingError::UnknownFunction)
        ));
    }
}

#[test]
fn one_accessor_overload_preserves_document_text_json_and_variant_domains() {
    let mut overload = None;
    for source in [
        physical(DataType::Utf8),
        json(),
        logical(DataType::LargeBinary, ValueLogicalType::Variant),
    ] {
        let arguments = [value(source.clone()), value(physical(DataType::Utf8))];
        let binding = resolve("get_json_int", FunctionKind::Scalar, &arguments);
        assert_eq!(
            binding.selected.argument_types[0],
            FunctionArgumentType::Value(source)
        );
        assert_eq!(output(&binding), &physical(DataType::Int64));
        if let Some(expected) = &overload {
            assert_eq!(&binding.selected.overload, expected);
        } else {
            overload = Some(binding.selected.overload);
        }
    }
    let arguments = [
        value(physical(DataType::LargeBinary)),
        value(physical(DataType::Utf8)),
    ];
    assert!(
        builtin_engine_function_catalog()
            .resolve_bound_user(
                "get_json_int",
                FunctionKind::Scalar,
                request(&arguments),
                crate::binding_test_control()
            )
            .is_err()
    );
}

#[test]
fn array_aggregate_final_json_and_ordered_physical_state_are_separate() {
    let catalog = builtin_engine_function_catalog();
    for ordered in [false, true] {
        let mut arguments = vec![value(json())];
        if ordered {
            arguments.push(value(physical(DataType::Int64)));
        }
        let request = FunctionBindingRequest {
            arguments: &arguments,
            logical_argument_count: 1,
            expected_result_type: None,
        };
        let binding = catalog
            .resolve_bound_user(
                "array_agg",
                FunctionKind::Aggregate,
                request,
                crate::binding_test_control(),
            )
            .unwrap();
        catalog
            .validate_bound(&binding, request, crate::binding_test_control())
            .unwrap();
        assert_eq!(output(&binding), &physical(DataType::List(json_item(true))));
        let state = &binding
            .selected
            .aggregate
            .as_ref()
            .unwrap()
            .intermediate_type;
        assert_eq!(state.logical_type(), ValueLogicalType::Physical);
        if ordered {
            let DataType::Struct(fields) = state.data_type() else {
                panic!("ordered state");
            };
            assert_eq!(fields.len(), 2);
            for (field, carrier) in fields.iter().zip([DataType::Utf8, DataType::Int64]) {
                let DataType::List(item) = field.data_type() else {
                    panic!("channel list");
                };
                assert_eq!(item.data_type(), &carrier);
                assert_eq!(
                    novarocks_type_contract::field_logical_type(item).unwrap(),
                    ValueLogicalType::Physical
                );
            }
        } else {
            let DataType::List(item) = state.data_type() else {
                panic!("state list");
            };
            assert_eq!(item.data_type(), &DataType::Utf8);
            assert_eq!(
                novarocks_type_contract::field_logical_type(item).unwrap(),
                ValueLogicalType::Physical
            );
        }
        let mut forged = binding;
        forged.selected.result_type = FunctionResultType::Scalar(physical(DataType::List(
            Arc::new(Field::new("item", DataType::Utf8, true)),
        )));
        assert!(
            catalog
                .validate_bound(&forged, request, crate::binding_test_control())
                .is_err()
        );
    }
}

#[test]
fn value_domain_aggregate_states_preserve_complete_source_identity() {
    for source in [
        json(),
        logical(DataType::LargeBinary, ValueLogicalType::Variant),
        physical(DataType::List(json_item(false))),
    ] {
        let arguments = [value(source.clone())];
        for name in ["min", "max", "any_value"] {
            let binding = resolve(name, FunctionKind::Aggregate, &arguments);
            let mut expected = source.clone();
            expected = expected
                .with_nullable(true)
                .expect("valid function nullability declaration");
            assert_eq!(output(&binding), &expected);
            assert_eq!(
                binding
                    .selected
                    .aggregate
                    .as_ref()
                    .unwrap()
                    .intermediate_type,
                expected
            );
            let mut forged = binding;
            forged
                .selected
                .aggregate
                .as_mut()
                .unwrap()
                .intermediate_type = physical(source.data_type().clone());
            if source.logical_type() != ValueLogicalType::Physical {
                assert!(
                    builtin_engine_function_catalog()
                        .validate_bound(&forged, request(&arguments), crate::binding_test_control())
                        .is_err()
                );
            }
        }
    }
}

#[test]
fn array_literal_and_sortby_preserve_complete_json_child_identity() {
    let arguments = [value(json()), value(json())];
    let literal = resolve("__array_literal", FunctionKind::Scalar, &arguments);
    assert_eq!(
        output(&literal),
        &physical(DataType::List(json_item(true)))
            .with_nullable(false)
            .unwrap()
    );
    assert!(!output(&literal).nullable());
    let mixed = [value(json()), value(physical(DataType::Utf8))];
    let text_literal = resolve_with_materialized_assignments("__array_literal", &mixed);
    assert_eq!(
        output(&text_literal).data_type().clone(),
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
    );
    assert_eq!(
        output(&text_literal).logical_type(),
        ValueLogicalType::Physical
    );
    assert!(!output(&text_literal).nullable());
    assert!(
        text_literal
            .selected
            .argument_types
            .iter()
            .all(|argument| argument == &FunctionArgumentType::Value(physical(DataType::Utf8)))
    );
    assert!(
        builtin_engine_function_catalog()
            .validate_bound(
                &text_literal,
                request(&mixed),
                crate::binding_test_control()
            )
            .is_err()
    );
    let list = physical(DataType::List(json_item(false)));
    let arguments = [
        value(list.clone()),
        value(physical(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Int64,
            true,
        ))))),
    ];
    let sorted = resolve("array_sortby", FunctionKind::Scalar, &arguments);
    assert_eq!(output(&sorted), &list);
    let mut forged = sorted;
    forged.selected.result_type = FunctionResultType::Scalar(physical(DataType::List(Arc::new(
        Field::new("item", DataType::Utf8, false),
    ))));
    assert!(
        builtin_engine_function_catalog()
            .validate_bound(&forged, request(&arguments), crate::binding_test_control())
            .is_err()
    );
}

#[test]
fn empty_array_instantiation_consumes_only_explicit_valid_list_constraint() {
    let catalog = builtin_engine_function_catalog();
    let default = resolve("__array_literal", FunctionKind::Scalar, &[]);
    assert_eq!(
        output(&default).data_type().clone(),
        DataType::List(Arc::new(Field::new("item", DataType::Null, true)))
    );
    let expected =
        FunctionValueType::from_legacy_declared_projection(DataType::List(json_item(false)), false);
    let explicit = FunctionBindingRequest {
        arguments: &[],
        logical_argument_count: 0,
        expected_result_type: Some(&expected),
    };
    let binding = catalog
        .resolve_bound_user(
            "__array_literal",
            FunctionKind::Scalar,
            explicit,
            crate::binding_test_control(),
        )
        .unwrap();
    assert_eq!(output(&binding), &expected);
    catalog
        .validate_bound(&binding, explicit, crate::binding_test_control())
        .unwrap();
    catalog
        .validate_bound(&binding, request(&[]), crate::binding_test_control())
        .unwrap();
    let conflicting = FunctionValueType::from_legacy_declared_projection(
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
        false,
    );
    assert!(
        catalog
            .validate_bound(
                &binding,
                FunctionBindingRequest {
                    expected_result_type: Some(&conflicting),
                    ..request(&[])
                },
                crate::binding_test_control()
            )
            .is_err()
    );
    assert!(
        FunctionValueType::try_from_legacy_declared_projection(
            DataType::List(Arc::new(
                Field::new("item", DataType::Utf8, true).with_metadata(
                    [(
                        novarocks_type_contract::NR_LOGICAL_TYPE_KEY.to_string(),
                        "variant".to_string(),
                    )]
                    .into(),
                ),
            )),
            false,
            ValueLogicalType::Physical,
        )
        .is_err()
    );
    for wrong in [physical(DataType::Utf8), json()] {
        assert!(
            catalog
                .resolve_bound_user(
                    "__array_literal",
                    FunctionKind::Scalar,
                    FunctionBindingRequest {
                        expected_result_type: Some(&wrong),
                        ..request(&[])
                    },
                    crate::binding_test_control()
                )
                .is_err()
        );
    }
    let arguments = [value(physical(DataType::Int64))];
    let binding = catalog
        .resolve_bound_user(
            "__array_literal",
            FunctionKind::Scalar,
            FunctionBindingRequest {
                expected_result_type: Some(&expected),
                ..request(&arguments)
            },
            crate::binding_test_control(),
        )
        .unwrap();
    let DataType::List(item) = &output(&binding).data_type() else {
        panic!("list result");
    };
    assert_eq!(item.data_type(), &DataType::Int64);
    assert_eq!(
        novarocks_type_contract::field_logical_type(item).unwrap(),
        ValueLogicalType::Physical
    );
}

#[test]
fn higher_order_array_result_carries_actual_lambda_result_domain() {
    let source = physical(DataType::List(json_item(true)));
    let arguments = [
        FunctionArgument::Lambda {
            parameter_types: novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
                vec![json()].into_boxed_slice(),
            ),
            result_type: json(),
        },
        value(source),
    ];
    let binding = resolve("array_map", FunctionKind::Scalar, &arguments);
    assert!(
        matches!(output(&binding).data_type(), DataType::List(item) if item.metadata().is_empty())
    );
    assert_eq!(
        output(&binding).child_at(0).unwrap().logical_type(),
        ValueLogicalType::Json
    );
    let wrong = [
        FunctionArgument::Lambda {
            parameter_types: novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
                vec![physical(DataType::Utf8)].into_boxed_slice(),
            ),
            result_type: json(),
        },
        arguments[1].clone(),
    ];
    assert!(
        builtin_engine_function_catalog()
            .resolve_bound_user(
                "array_map",
                FunctionKind::Scalar,
                request(&wrong),
                crate::binding_test_control()
            )
            .is_err()
    );
}

fn variant_get_arguments(source: FunctionValueType, typed: bool) -> Vec<FunctionArgument> {
    let text = |raw| {
        let constant = super::constant_binding_tests::utf8(raw, false);
        FunctionArgument::Value {
            value_type: constant.value_type().clone(),
            constant: Some(constant),
        }
    };
    let mut args = vec![value(source), text("$.a")];
    if typed {
        args.push(text("bigint"));
    }
    args
}

#[test]
fn variant_get_json_text_and_nominal_inputs_keep_complete_fresh_and_frozen_domains() {
    let catalog = builtin_engine_function_catalog();
    for name in ["variant_get", "try_variant_get"] {
        for source in [
            physical(DataType::Utf8),
            json(),
            logical(DataType::LargeBinary, ValueLogicalType::Variant),
        ] {
            for typed in [false, true] {
                let args = variant_get_arguments(source.clone(), typed);
                let binding = resolve(name, FunctionKind::Scalar, &args);
                assert_eq!(
                    binding.selected.argument_types[0],
                    FunctionArgumentType::Value(source.clone())
                );
                catalog
                    .validate_frozen_selection(
                        &binding.function_id,
                        binding.kind,
                        &binding.selected,
                        request(&args),
                        crate::binding_test_control(),
                    )
                    .unwrap();
                let expected = if typed {
                    physical(DataType::Int64)
                } else {
                    logical(DataType::LargeBinary, ValueLogicalType::Variant)
                };
                assert_eq!(output(&binding), &expected);
                // A binding for validated Json and a binding for ordinary text
                // must never be replayed against one another's original fact.
                if source.data_type() == &DataType::Utf8 {
                    let other = if source.logical_type() == ValueLogicalType::Json {
                        physical(DataType::Utf8)
                    } else {
                        json()
                    };
                    let foreign = variant_get_arguments(other, typed);
                    assert!(
                        catalog
                            .validate_frozen_selection(
                                &binding.function_id,
                                binding.kind,
                                &binding.selected,
                                request(&foreign),
                                crate::binding_test_control()
                            )
                            .is_err()
                    );
                }
            }
        }
    }
}

#[test]
fn variant_get_text_mode_does_not_admit_raw_bytes_wide_text_or_foreign_nominal_domains() {
    let catalog = builtin_engine_function_catalog();
    let rejected = [
        physical(DataType::Binary),
        physical(DataType::LargeBinary),
        physical(DataType::LargeUtf8),
        logical(DataType::Binary, ValueLogicalType::Hll),
        logical(DataType::Binary, ValueLogicalType::Bitmap),
        logical(DataType::Binary, ValueLogicalType::Percentile),
        logical(DataType::FixedSizeBinary(16), ValueLogicalType::Uuid),
    ];
    for name in ["variant_get", "try_variant_get"] {
        for source in &rejected {
            for typed in [false, true] {
                let args = variant_get_arguments(source.clone(), typed);
                assert!(
                    catalog
                        .resolve_bound_user(
                            name,
                            FunctionKind::Scalar,
                            request(&args),
                            crate::binding_test_control()
                        )
                        .is_err(),
                    "{name}: {source:?}"
                );
            }
        }
    }
}
