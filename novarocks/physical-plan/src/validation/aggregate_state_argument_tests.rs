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
use novarocks_type_contract::{AggregateStateArgumentContract, FunctionArgumentType};
use std::sync::Arc;

fn binding(ty: crate::ValueType) -> crate::AggregateBinding {
    crate::AggregateBinding {
        state_interpretation: None,
        state_argument_contract: AggregateStateArgumentContract::ValueRootNullabilityIndependent,
        function: crate::BoundFunction {
            function_id: novarocks_type_contract::FunctionId::try_new("test/state-argument-owner")
                .unwrap(),
            overload: novarocks_type_contract::FunctionOverloadId::try_new("domain-v1").unwrap(),
            kind: novarocks_type_contract::FunctionKind::Aggregate,
            argument_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from([
                FunctionArgumentType::Value(ty),
            ])),
            result_type: crate::ValueType::new(arrow_schema::DataType::Int64, false),
            legacy_metadata: Some(crate::LegacyBindingMetadata {
                volatility: novarocks_type_contract::FunctionVolatility::Immutable,
                argument_evaluation: novarocks_type_contract::FunctionArgumentEvaluation::Eager,
                failure_behavior: novarocks_type_contract::FunctionFailureBehavior::Propagate,
                intrinsic_row_error:
                    novarocks_type_contract::FunctionIntrinsicRowError::NotRowEvaluated,
                semantic_parameters: Box::default(),
            }),
        },
        phase: crate::AggregatePhase::Final {
            sequence: crate::AggregateSequenceId::new(9),
        },
        logical_argument_count: 1,
        intermediate_type: crate::ValueType::new(arrow_schema::DataType::Int64, false),
        state_format: novarocks_type_contract::AggregateStateFormatId::try_new(
            "test/domain-state-v1",
        )
        .unwrap(),
    }
}
fn matches(a: &crate::AggregateBinding, b: &crate::AggregateBinding) -> bool {
    aggregate_bindings_match(
        a,
        b,
        &mut SemanticTraceWorkBudget::new(&crate::PlanLimits::default()),
    )
}
fn argument(binding: &crate::AggregateBinding) -> &crate::ValueType {
    let FunctionArgumentType::Value(value) = &binding.function.argument_types[0] else {
        unreachable!()
    };
    value
}
fn set_argument(binding: &mut crate::AggregateBinding, value: crate::ValueType) {
    let mut arguments = binding.function.argument_types.to_vec();
    arguments[0] = FunctionArgumentType::Value(value);
    binding.function.argument_types =
        novarocks_type_contract::TypedFactSlice::from_pure_boxed(arguments.into_boxed_slice());
}

#[test]
fn state_contract_only_relaxes_logical_value_root_nullability() {
    let a = binding(crate::ValueType::new(arrow_schema::DataType::Utf8, false));
    let mut b = a.clone();
    let nullable = argument(&b).with_nullable(true).unwrap();
    set_argument(&mut b, nullable);
    assert!(matches(&a, &b));
    assert!(!a.function.signature_matches(&b.function));
    let wide = binding(crate::ValueType::new(
        arrow_schema::DataType::LargeUtf8,
        false,
    ));
    assert!(
        !matches(&a, &wide),
        "root-null admission does not rewrite physical offset width"
    );
    let mut exact_a = a.clone();
    let mut exact_b = b.clone();
    exact_a.state_argument_contract = AggregateStateArgumentContract::ExactSignature;
    exact_b.state_argument_contract = AggregateStateArgumentContract::ExactSignature;
    assert!(!matches(&exact_a, &exact_b));
    assert!(!matches(&a, &exact_b));
    let mut nominal = b.clone();
    set_argument(
        &mut nominal,
        crate::ValueType::try_with_logical_type(
            arrow_schema::DataType::Utf8,
            true,
            novarocks_type_contract::ValueLogicalType::Json,
        )
        .unwrap(),
    );
    assert!(!matches(&a, &nominal));
    let mut order_a = a.clone();
    let mut order_b = b.clone();
    order_a.function.argument_types =
        novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from([
            a.function.argument_types[0].clone(),
            a.function.argument_types[0].clone(),
        ]));
    order_b.function.argument_types =
        novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from([
            b.function.argument_types[0].clone(),
            b.function.argument_types[0].clone(),
        ]));
    assert!(!matches(&order_a, &order_b));
    let lambda = |nullable| FunctionArgumentType::Lambda {
        parameter_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from([
            crate::ValueType::new(arrow_schema::DataType::Utf8, nullable),
        ])),
        result_type: crate::ValueType::new(arrow_schema::DataType::Utf8, false),
    };
    let mut lambda_a = a.clone();
    let mut lambda_b = a.clone();
    lambda_a.function.argument_types =
        novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from([lambda(false)]));
    lambda_b.function.argument_types =
        novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from([lambda(true)]));
    assert!(!matches(&lambda_a, &lambda_b));
    for mutation in 0..6 {
        let mut changed = b.clone();
        match mutation {
            0 => {
                changed.state_format =
                    novarocks_type_contract::AggregateStateFormatId::try_new("test/other-state-v1")
                        .unwrap()
            }
            1 => changed.intermediate_type = changed.intermediate_type.with_nullable(true).unwrap(),
            2 => {
                changed.function.result_type =
                    changed.function.result_type.with_nullable(true).unwrap()
            }
            3 => changed.logical_argument_count = 0,
            4 => {
                changed.phase = crate::AggregatePhase::Final {
                    sequence: crate::AggregateSequenceId::new(10),
                }
            }
            5 => {
                changed.function.overload =
                    novarocks_type_contract::FunctionOverloadId::try_new("other-v1").unwrap()
            }
            _ => unreachable!(),
        }
        assert!(!matches(&a, &changed), "mutation {mutation}");
    }
}

#[test]
fn state_contract_preserves_nested_semantics_and_charges_actual_type_work() {
    use arrow_schema::{DataType, Field};
    let fields: Vec<_> = (0..320)
        .map(|index| {
            Arc::new(
                Field::new(format!("field_{index}"), DataType::Utf8, false).with_metadata(
                    std::collections::HashMap::<String, String>::from([(
                        "provider.id".to_owned(),
                        index.to_string(),
                    )]),
                ),
            )
        })
        .collect();
    let a = binding(crate::ValueType::new(
        DataType::Struct(fields.clone().into()),
        false,
    ));
    let mut b = a.clone();
    let nullable = argument(&b).with_nullable(true).unwrap();
    set_argument(&mut b, nullable);
    let mut wide_budget = SemanticTraceWorkBudget { remaining: 1 << 20 };
    assert!(aggregate_bindings_match(&a, &b, &mut wide_budget));
    let charged = (1 << 20) - wide_budget.remaining;
    assert!(
        charged > 960,
        "actual semantic fields/names and physical layout were observed"
    );
    let mut short_budget = SemanticTraceWorkBudget {
        remaining: charged - 1,
    };
    assert!(!aggregate_bindings_match(&a, &b, &mut short_budget));
    let mut exact_budget = SemanticTraceWorkBudget { remaining: charged };
    assert!(aggregate_bindings_match(&a, &b, &mut exact_budget));
    assert_eq!(exact_budget.remaining, 0);
    for (index, field) in fields.iter().enumerate() {
        for mutation in 0..2 {
            let mut changed = fields.clone();
            changed[index] = Arc::new(match mutation {
                0 => field.as_ref().clone().with_nullable(true),
                1 => field
                    .as_ref()
                    .clone()
                    .with_metadata(std::collections::HashMap::<String, String>::from([(
                        "provider.id".to_owned(),
                        "foreign".to_owned(),
                    )])),
                _ => unreachable!(),
            });
            let mut wrong = b.clone();
            set_argument(
                &mut wrong,
                crate::ValueType::new(DataType::Struct(changed.into()), true),
            );
            assert_eq!(
                matches(&a, &wrong),
                mutation == 1,
                "field {index}, mutation {mutation}"
            );
        }
    }
}

#[test]
fn state_contract_preserves_nested_nominal_domain_when_carriers_are_identical() {
    use arrow_schema::{DataType, Field};
    use novarocks_type_contract::{
        ValueField, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef,
        ValueTypeStorageLifetime,
    };
    let layout = DataType::Struct(
        vec![Arc::new(Field::new(
            "payload",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ))]
        .into(),
    );
    let plain = crate::ValueType::new(layout.clone(), false);
    let storage = ValueTypeStorageLifetime::pure();
    let child = ValueTypeRef::try_new(
        ValueKind::Nominal(ValueLogicalType::Json),
        true,
        storage.clone(),
    )
    .unwrap();
    let list = ValueTypeRef::try_new(ValueKind::List(child), false, storage.clone()).unwrap();
    let value = ValueTypeRef::try_new(
        ValueKind::Struct(
            vec![ValueField::new("payload", list, storage.clone())].into_boxed_slice(),
        ),
        false,
        storage.clone(),
    )
    .unwrap();
    let nominal =
        crate::ValueType::try_from_value(value, ValueLayout::try_new(&layout, storage).unwrap())
            .unwrap();
    assert_eq!(plain.data_type(), nominal.data_type());
    assert_eq!(plain.logical_type(), nominal.logical_type());
    assert!(!matches(&binding(plain), &binding(nominal.clone())));
    assert!(matches(
        &binding(nominal.clone()),
        &binding(nominal.with_nullable(true).unwrap())
    ));
}

#[test]
fn special_string_map_profile_cannot_accept_nested_json_by_arrow_shape() {
    use arrow_schema::{DataType, Field};
    use novarocks_type_contract::{
        ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef, ValueTypeStorageLifetime,
    };
    let storage = ValueTypeStorageLifetime::pure();
    let semantic = ValueTypeRef::try_new(
        ValueKind::Map {
            key: ValueTypeRef::try_new(ValueKind::String, false, storage.clone()).unwrap(),
            value: ValueTypeRef::try_new(
                ValueKind::Nominal(ValueLogicalType::Json),
                false,
                storage.clone(),
            )
            .unwrap(),
        },
        false,
        storage.clone(),
    )
    .unwrap();
    let layout = DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Utf8, false),
                ]
                .into(),
            ),
            false,
        )),
        false,
    );
    let nominal =
        crate::ValueType::try_from_value(semantic, ValueLayout::try_new(&layout, storage).unwrap())
            .unwrap();
    let constant = crate::UnpivotConstant::Utf8Map(crate::ConstantReference {
        pool: crate::ConstantPoolId::new(0),
        ordinal: 0,
    });
    assert!(super::super::node::unpivot_collection_carrier_matches(
        &constant,
        nominal.data_type()
    ));
    assert!(!super::super::node::unpivot_collection_binding_matches(
        &constant, &nominal
    ));
}
