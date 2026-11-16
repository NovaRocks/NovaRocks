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

fn calls<'a>(
    owner: &'a SqlAuthoredPhysicalPlan,
    id: &str,
) -> Vec<(&'a Fragment, &'a novarocks_physical_plan::ExprNode)> {
    owner
        .plan()
        .fragments()
        .values()
        .flat_map(|fragment| {
            fragment
                .expressions()
                .iter()
                .filter_map(move |(_, source)| match &source.kind {
                    ContractExprKind::FunctionCall { function, .. }
                        if function.function_id.as_str() == id =>
                    {
                        Some((fragment, source))
                    }
                    _ => None,
                })
        })
        .collect()
}

fn assert_constraint<'a>(
    owner: &'a SqlAuthoredPhysicalPlan,
    fragment: &'a Fragment,
    source: &'a novarocks_physical_plan::ExprNode,
) -> CheckedExpressionLogicalSourceEntry<'a> {
    let receipt = loan(owner, fragment, source, &Control::default()).unwrap();
    let target = receipt
        .captured()
        .binding()
        .result_constraint()
        .expect("explicit result target retained by its original producer");
    let selected = receipt
        .canonical_selection()
        .expect("same-emission selected scalar owner");
    assert_eq!(
        selected.result_type,
        FunctionResultType::Scalar(target.clone())
    );
    assert_eq!(&source.ty, target);
    let ContractExprKind::FunctionCall { function, .. } = &source.kind else {
        unreachable!()
    };
    assert_eq!(&function.result_type, target);
    assert_eq!(function.argument_types, selected.argument_types);
    assert_eq!(function.overload, selected.overload);
    assert_eq!(
        function.function_id,
        receipt.captured().binding().resolved().function_id
    );
    let operational = operational_arguments(owner, &receipt, &Control::default()).unwrap();
    assert_eq!(
        selected.argument_types.as_ref(),
        operational
            .iter()
            .map(FunctionArgument::argument_type)
            .collect::<Vec<_>>()
            .as_slice()
    );
    let cloned = owner.clone();
    let again = loan(&cloned, fragment, source, &Control::default()).unwrap();
    assert!(std::ptr::eq(
        target,
        again.captured().binding().result_constraint().unwrap()
    ));
    assert!(std::ptr::eq(receipt.captured(), again.captured()));
    assert!(Arc::ptr_eq(selected, again.canonical_selection().unwrap()));
    assert_eq!(
        receipt.captured().binding().decimal_overflow_policy(),
        again.captured().binding().decimal_overflow_policy()
    );
    assert_eq!(
        receipt.captured().constant_policy(),
        again.captured().constant_policy()
    );
    receipt
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
    source: &novarocks_physical_plan::ExprNode,
) {
    let control = Control::default();
    loan(owner, fragment, source, &control).unwrap();
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
            assert!(matches!(loan(owner, fragment, source, &control),
                Err(SqlSourceJournalError::Control(actual)) if actual == cause));
            assert_eq!(control.trace(), baseline[..=stop]);
        }
    }
}

#[test]
fn canonical_constraint_actual_json_text_and_nested_conversion_keep_explicit_target() {
    // The first query is the existing terminal JSON aggregation regression,
    // with a real table added for this catalog/statistics/provider driver.
    for (sql, nested) in [
        (
            "SELECT array_agg(json_object('2:3')) AS j, array_agg(CAST(json_object('2:3') AS VARCHAR)) AS s FROM orders",
            false,
        ),
        (
            "SELECT CAST(array_sortby([json_object('k',1), json_object('k',2)], [2,1]) AS ARRAY<VARCHAR>) AS s FROM orders",
            true,
        ),
    ] {
        let owner = crate::compiler::compile_authored_aggregate_for_test(sql);
        let conversions = calls(&owner, CONVERSION);
        if nested {
            // Explicit array CAST owns JSON conversion; the scalar None-mode
            // byte conversion must not consume or retag its source first.
            assert!(conversions.is_empty());
            let mut cast_count = 0;
            for fragment in owner.plan().fragments().values() {
                for (_, expression) in fragment.expressions().iter() {
                    if let ContractExprKind::Cast {
                        expr,
                        array_failure_mode: Some(mode),
                        target,
                        ..
                    } = &expression.kind
                    {
                        cast_count += 1;
                        assert_eq!(
                            *mode,
                            novarocks_type_contract::ArrayCastFailureMode::ReportError
                        );
                        assert_eq!(target, expression.ty.data_type());
                        let source = fragment.expressions().get(*expr).unwrap();
                        assert_eq!(
                            source.ty.child_at(0).unwrap().logical_type(),
                            ValueLogicalType::Json
                        );
                        assert_eq!(
                            expression.ty.child_at(0).unwrap().logical_type(),
                            ValueLogicalType::Physical
                        );
                        assert_ne!(source.ty, expression.ty);
                        let ContractExprKind::FunctionCall { function, .. } = &source.kind else {
                            panic!("original array_sortby source");
                        };
                        assert_eq!(
                            function.function_id.as_str(),
                            "builtin.scalar/array_sortby/v1"
                        );
                        assert_eq!(function.result_type, source.ty);
                        let receipt = loan(&owner, fragment, source, &Control::default()).unwrap();
                        assert_eq!(
                            receipt.canonical_selection().unwrap().result_type,
                            FunctionResultType::Scalar(source.ty.clone())
                        );
                        assert_loan_prefixes(&owner, fragment, source);
                    }
                }
            }
            assert!(cast_count >= 1);
            let arrays = calls(&owner, ARRAY_LITERAL);
            assert!(!arrays.is_empty());
            let mut json_values = 0;
            let mut sort_keys = 0;
            for (fragment, source) in arrays {
                let receipt = loan(&owner, fragment, source, &Control::default()).unwrap();
                assert_eq!(
                    receipt.canonical_selection().unwrap().result_type,
                    FunctionResultType::Scalar(source.ty.clone())
                );
                let DataType::List(item) = source.ty.data_type() else {
                    panic!("original array source");
                };
                let domain = source.ty.child_at(0).unwrap().logical_type();
                match (item.data_type(), domain) {
                    (DataType::Utf8, ValueLogicalType::Json) => json_values += 1,
                    (DataType::Int32, ValueLogicalType::Physical) => sort_keys += 1,
                    actual => panic!("unexpected array source {actual:?}"),
                }
                assert_eq!(receipt.captured().request().logical_argument_count, 2);
                assert_loan_prefixes(&owner, fragment, source);
            }
            assert!(json_values >= 1, "original JSON values source retained");
            assert!(sort_keys >= 1, "original Int32 sort key source retained");
        } else {
            assert!(
                !conversions.is_empty(),
                "actual scalar logical-domain conversion must survive"
            );
        }
        for (fragment, source) in conversions {
            let receipt = assert_constraint(&owner, fragment, source);
            let original = receipt.captured().request();
            assert_eq!(original.arguments.len(), 1);
            let FunctionArgument::Value {
                value_type,
                constant: None,
            } = &original.arguments[0]
            else {
                panic!("original computed JSON source must remain None");
            };
            let target = receipt.captured().binding().result_constraint().unwrap();
            assert_eq!(target.logical_type(), ValueLogicalType::Physical);
            assert_eq!(value_type.logical_type(), ValueLogicalType::Json);
            assert_eq!(target.data_type().clone(), DataType::Utf8);
            let operational = operational_arguments(&owner, &receipt, &Control::default()).unwrap();
            assert!(constant(&operational[0]).is_none());
            assert_loan_prefixes(&owner, fragment, source);
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
    let (fragment, source) = ordinary[0];
    let receipt = loan(&owner, fragment, source, &Control::default()).unwrap();
    assert!(receipt.captured().request().expected_result_type.is_some());
    assert!(
        receipt.captured().binding().result_constraint().is_none(),
        "an inferred original selected result is not a producer constraint"
    );
    assert!(receipt.canonical_selection().is_some());
}

#[test]
fn typed_empty_array_retains_independent_unknown_owner_and_outer_json_cast() {
    for sql in [
        "SELECT ARRAY<JSON>[] AS a FROM orders",
        "SELECT CAST([] AS ARRAY<JSON>) AS a FROM orders",
    ] {
        let owner = crate::compiler::compile_authored_aggregate_for_test(sql);
        let arrays = calls(&owner, ARRAY_LITERAL);
        assert_eq!(arrays.len(), 1);
        let (fragment, source) = arrays[0];
        let receipt = loan(&owner, fragment, source, &Control::default()).unwrap();
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
        for (_, expression) in fragment.expressions().iter() {
            if let ContractExprKind::Cast {
                expr,
                target,
                array_failure_mode: Some(mode),
                ..
            } = &expression.kind
            {
                if fragment.expressions().get(*expr).unwrap().id != source.id {
                    continue;
                }
                json_casts += 1;
                assert_eq!(
                    *mode,
                    novarocks_type_contract::ArrayCastFailureMode::ReportError
                );
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
        assert_loan_prefixes(&owner, fragment, source);
    }
}
