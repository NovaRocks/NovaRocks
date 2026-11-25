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
use novarocks_type_contract::ValueLogicalType;

const CONVERSION: &str =
    novarocks_functions::builtin::value_conversion::VALUE_CONVERSION_FUNCTION_ID;
const ARRAY_LITERAL: &str = "builtin.scalar/__array_literal/v1";
const CONCAT: &str = "builtin.scalar/concat/v1";

fn calls<'a>(
    owner: &'a SqlAuthoredPhysicalPlan,
    id: &str,
) -> Vec<(
    &'a Fragment,
    &'a novarocks_physical_plan::PhysicalNode,
    &'a novarocks_expression_contract::ExprNode,
)> {
    owner
        .plan()
        .fragments()
        .values()
        .flat_map(|fragment| {
            fragment.nodes().values().flat_map(move |node| {
                node.expressions.iter().flat_map(move |unit| {
                    unit.definitions()
                        .iter()
                        .filter_map(move |source| match &source.kind {
                            ContractExprKind::FunctionCall { function, .. }
                                if function.function_id.as_str() == id =>
                            {
                                Some((fragment, node, source))
                            }
                            _ => None,
                        })
                })
            })
        })
        .collect()
}

fn assert_public_result_fields(owner: &SqlAuthoredPhysicalPlan) {
    let result = owner.plan().result_port().unwrap();
    let fragment = owner.plan().fragments().get(&result.fragment).unwrap();
    assert!(!result.fields.is_empty());
    for field in &result.fields {
        assert_eq!(field.ty, fragment.values().get(&field.value).unwrap().ty);
        assert!(result.output.columns.contains(&field.value));
    }
}

fn assert_loan_prefixes(
    owner: &SqlAuthoredPhysicalPlan,
    fragment: &Fragment,
    unit_node: &novarocks_physical_plan::PhysicalNode,
    source: &novarocks_expression_contract::ExprNode,
) {
    let control = Control::default();
    loan(owner, fragment, unit_node, source, &control).unwrap();
    let baseline = control.trace();
    assert_eq!(baseline[0], (CompilePhase::Validate, 0));
    for stop in 0..baseline.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                refusal: Some((stop, cause.clone())),
                ..Control::default()
            };
            assert!(matches!(loan(owner, fragment, unit_node, source, &control),
                Err(SqlSourceJournalError::Control(actual)) if actual == cause));
            assert_eq!(control.trace(), baseline[..=stop]);
        }
    }
}

#[test]
fn explicit_json_text_casts_keep_their_target_and_original_scalar_sources() {
    // The first query is the existing terminal JSON aggregation regression,
    // with a real table added for this catalog/statistics/provider driver.
    for (sql, nested) in [
        (
            "SELECT array_agg(JSON_OBJECT('2' VALUE 3 RETURNING JSON)) AS j, array_agg(CAST(JSON_OBJECT('2' VALUE 3 RETURNING JSON) AS VARCHAR)) AS s FROM orders",
            false,
        ),
        (
            "SELECT CAST(concat(
                ARRAY[JSON_OBJECT('k' VALUE order_key RETURNING JSON), JSON_OBJECT('m' VALUE order_key RETURNING JSON)],
                ARRAY[JSON_OBJECT('n' VALUE order_key RETURNING JSON), JSON_OBJECT('p' VALUE order_key RETURNING JSON)]
             ) AS ARRAY(VARCHAR)) AS s FROM orders",
            true,
        ),
    ] {
        let owner = crate::compiler::compile_authored_aggregate_for_test(sql);
        assert!(calls(&owner, CONVERSION).is_empty());
        if nested {
            // Explicit array CAST owns JSON conversion; the scalar None-mode
            // byte conversion must not consume or retag its source first.
            let mut cast_count = 0;
            for (fragment, unit_node, unit) in
                owner.plan().fragments().values().flat_map(|fragment| {
                    fragment.nodes().values().filter_map(move |node| {
                        node.expressions.as_ref().map(|unit| (fragment, node, unit))
                    })
                })
            {
                for expression in unit.definitions() {
                    if let ContractExprKind::Cast {
                        expr,
                        cast_mode: mode,
                        target,
                        ..
                    } = &expression.kind
                    {
                        cast_count += 1;
                        assert_eq!(*mode, novarocks_type_contract::TrinoCastMode::Explicit);
                        assert_eq!(target, expression.ty.data_type());
                        let source = unit.definition(*expr).unwrap();
                        assert_eq!(
                            source.ty.child_at(0).unwrap().logical_type(),
                            ValueLogicalType::Json
                        );
                        assert_eq!(
                            expression.ty.child_at(0).unwrap().logical_type(),
                            ValueLogicalType::Physical
                        );
                        assert_ne!(source.ty, expression.ty);
                        let ContractExprKind::FunctionCall { function, args } = &source.kind else {
                            panic!("original array concat source");
                        };
                        assert_eq!(function.function_id.as_str(), CONCAT);
                        assert_eq!(args.len(), 2);
                        assert_eq!(function.result_type, source.ty);
                        let receipt =
                            loan(&owner, fragment, unit_node, source, &Control::default()).unwrap();
                        assert_eq!(
                            receipt.canonical_selection().unwrap().result_type,
                            FunctionResultType::Scalar(source.ty.clone())
                        );
                        let original = receipt.captured().request();
                        assert_eq!(original.logical_argument_count, 2);
                        assert_eq!(original.arguments.len(), 2);
                        for argument in original.arguments.iter() {
                            let FunctionArgument::Value {
                                value_type,
                                constant: None,
                            } = argument
                            else {
                                panic!(
                                    "original concat argument must remain a computed JSON array"
                                );
                            };
                            let item = value_type.child_at(0).unwrap();
                            assert_eq!(item.data_type(), &DataType::LargeBinary);
                            assert_eq!(item.logical_type(), ValueLogicalType::Json);
                        }
                        assert_loan_prefixes(&owner, fragment, unit_node, source);
                    }
                }
            }
            assert!(cast_count >= 1);
            let arrays = calls(&owner, ARRAY_LITERAL);
            assert!(
                arrays.len() >= 2,
                "both original JSON array sources retained"
            );
            for (fragment, unit_node, source) in arrays {
                let receipt =
                    loan(&owner, fragment, unit_node, source, &Control::default()).unwrap();
                assert_eq!(
                    receipt.canonical_selection().unwrap().result_type,
                    FunctionResultType::Scalar(source.ty.clone())
                );
                let DataType::List(item) = source.ty.data_type() else {
                    panic!("original array source");
                };
                assert_eq!(item.data_type(), &DataType::LargeBinary);
                assert_eq!(
                    source.ty.child_at(0).unwrap().logical_type(),
                    ValueLogicalType::Json
                );
                let original = receipt.captured().request();
                assert_eq!(original.logical_argument_count, 2);
                assert_eq!(original.arguments.len(), 2);
                for argument in original.arguments.iter() {
                    let FunctionArgument::Value {
                        value_type,
                        constant: None,
                    } = argument
                    else {
                        panic!("original array element must remain computed JSON");
                    };
                    assert_eq!(value_type.logical_type(), ValueLogicalType::Json);
                }
                assert_loan_prefixes(&owner, fragment, unit_node, source);
            }
        } else {
            let mut casts = 0;
            for (fragment, node, unit) in owner.plan().fragments().values().flat_map(|fragment| {
                fragment.nodes().values().filter_map(move |node| {
                    node.expressions.as_ref().map(|unit| (fragment, node, unit))
                })
            }) {
                for expression in unit.definitions() {
                    let ContractExprKind::Cast {
                        expr,
                        target: DataType::Utf8,
                        cast_mode: novarocks_type_contract::TrinoCastMode::Explicit,
                    } = &expression.kind
                    else {
                        continue;
                    };
                    let original = unit.definition(*expr).unwrap();
                    assert_eq!(original.ty.logical_type(), ValueLogicalType::Json);
                    assert_eq!(expression.ty.logical_type(), ValueLogicalType::Physical);
                    let ContractExprKind::FunctionCall { function, .. } = &original.kind else {
                        panic!("expected the original JSON object call");
                    };
                    assert_eq!(
                        function.function_id.as_str(),
                        "builtin.scalar/__json_object_null/v1"
                    );
                    assert_eq!(function.result_type, original.ty);
                    let receipt =
                        loan(&owner, fragment, node, original, &Control::default()).unwrap();
                    let selected = receipt.canonical_selection().unwrap();
                    assert_eq!(
                        selected.result_type,
                        FunctionResultType::Scalar(original.ty.clone())
                    );
                    assert_eq!(function.argument_types, selected.argument_types);
                    assert!(receipt.captured().binding().result_constraint().is_none());
                    assert_loan_prefixes(&owner, fragment, node, original);
                    casts += 1;
                }
            }
            assert!(
                casts >= 1,
                "the physical plan must retain the explicit JSON CAST"
            );
        }
        assert_public_result_fields(&owner);
        let result = owner.plan().result_port().unwrap();
        let DataType::List(item) = &result.fields.last().unwrap().ty.data_type() else {
            panic!("independent final ARRAY<VARCHAR> schema");
        };
        assert_eq!(item.data_type(), &DataType::Utf8);
        assert_eq!(
            result
                .fields
                .last()
                .unwrap()
                .ty
                .child_at(0)
                .unwrap()
                .logical_type(),
            ValueLogicalType::Physical
        );
    }

    let owner = crate::compiler::compile_authored_aggregate_for_test(
        "SELECT LOWER(CAST(order_key AS VARCHAR)) AS s FROM orders",
    );
    let ordinary = calls(&owner, "builtin.scalar/lower/v1");
    assert_eq!(ordinary.len(), 1);
    let (fragment, unit_node, source) = ordinary[0];
    let receipt = loan(&owner, fragment, unit_node, source, &Control::default()).unwrap();
    assert!(receipt.captured().request().expected_result_type.is_some());
    assert!(
        receipt.captured().binding().result_constraint().is_none(),
        "an inferred original selected result is not a producer constraint"
    );
    assert!(receipt.canonical_selection().is_some());
}

#[test]
fn typed_empty_array_retains_independent_unknown_owner_and_outer_json_cast() {
    {
        let owner = crate::compiler::compile_authored_aggregate_for_test(
            "SELECT CAST(ARRAY[] AS ARRAY(JSON)) AS a FROM orders",
        );
        let arrays = calls(&owner, ARRAY_LITERAL);
        assert_eq!(arrays.len(), 1);
        let (fragment, unit_node, source) = arrays[0];
        let receipt = loan(&owner, fragment, unit_node, source, &Control::default()).unwrap();
        assert!(receipt.arguments().is_empty());
        assert!(receipt.captured().request().arguments.is_empty());
        assert_eq!(receipt.captured().request().logical_argument_count, 0);
        assert!(receipt.captured().binding().result_constraint().is_none());
        let selected = receipt.canonical_selection().unwrap();
        assert_eq!(
            selected.result_type,
            FunctionResultType::Scalar(source.ty.clone())
        );
        assert_eq!(source.ty.child_at(0).unwrap().data_type(), &DataType::Null);
        assert_eq!(
            source.ty.child_at(0).unwrap().logical_type(),
            ValueLogicalType::Physical
        );
        let mut json_casts = 0;
        let unit = unit_node.expressions.as_ref().unwrap();
        for expression in unit.definitions() {
            if let ContractExprKind::Cast {
                expr,
                target,
                cast_mode: mode,
                ..
            } = &expression.kind
            {
                if unit.definition(*expr).unwrap().id != source.id {
                    continue;
                }
                json_casts += 1;
                assert_eq!(*mode, novarocks_type_contract::TrinoCastMode::Explicit);
                assert_eq!(target, expression.ty.data_type());
                let DataType::List(item) = expression.ty.data_type() else {
                    panic!("outer JSON list target");
                };
                assert_eq!(item.name(), "item");
                assert_eq!(item.data_type(), &DataType::Utf8);
                assert!(item.is_nullable());
                assert!(item.metadata().is_empty());
                assert_eq!(
                    expression.ty.child_at(0).unwrap().logical_type(),
                    ValueLogicalType::Json
                );
                assert_public_result_fields(&owner);
                let result = owner.plan().result_port().unwrap();
                assert_eq!(result.fields.len(), 1);
                assert_eq!(&result.fields[0].ty, &expression.ty);
            }
        }
        assert_eq!(json_casts, 1);
        assert_loan_prefixes(&owner, fragment, unit_node, source);
    }
}
