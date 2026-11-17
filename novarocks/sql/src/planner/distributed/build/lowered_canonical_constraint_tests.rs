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

fn assert_constraint<'a>(
    owner: &'a SqlAuthoredPhysicalPlan,
    fragment: &'a Fragment,
    unit_node: &'a novarocks_physical_plan::PhysicalNode,
    source: &'a novarocks_expression_contract::ExprNode,
) -> CheckedExpressionLogicalSourceEntry<'a> {
    let receipt = loan(owner, fragment, unit_node, source, &Control::default()).unwrap();
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
    let again = loan(&cloned, fragment, unit_node, source, &Control::default()).unwrap();
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
        assert!(
            !conversions.is_empty(),
            "actual logical-domain conversion must survive"
        );
        for (fragment, unit_node, source) in conversions {
            let receipt = assert_constraint(&owner, fragment, unit_node, source);
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
            if nested {
                let DataType::List(before) = &value_type.data_type() else {
                    panic!("JSON list input");
                };
                let DataType::List(after) = &target.data_type() else {
                    panic!("text list target");
                };
                assert_eq!(
                    value_type.child_at(0).unwrap().logical_type(),
                    ValueLogicalType::Json
                );
                assert_eq!(
                    target.child_at(0).unwrap().logical_type(),
                    ValueLogicalType::Physical
                );
                assert_eq!(before.name(), after.name());
                assert_eq!(before.is_nullable(), after.is_nullable());
                assert_eq!(before.data_type(), &DataType::Utf8);
                assert_eq!(after.data_type(), &DataType::Utf8);
                assert!(before.metadata().is_empty());
                assert!(after.metadata().is_empty());
            } else {
                assert_eq!(value_type.logical_type(), ValueLogicalType::Json);
                assert_eq!(target.data_type().clone(), DataType::Utf8);
            }
            let operational = operational_arguments(&owner, &receipt, &Control::default()).unwrap();
            assert!(constant(&operational[0]).is_none());
            assert_loan_prefixes(&owner, fragment, unit_node, source);
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
fn canonical_constraint_actual_typed_empty_array_retains_full_json_item_target() {
    // ARRAY<JSON>[] is the parser's explicit typed-array vocabulary, not a
    // CAST([]) shortcut or a manually synthesized checked journal token.
    let owner = crate::compiler::compile_authored_aggregate_for_test(
        "SELECT ARRAY<JSON>[] AS a FROM orders",
    );
    let arrays = calls(&owner, ARRAY_LITERAL);
    assert_eq!(arrays.len(), 1);
    let (fragment, unit_node, source) = arrays[0];
    let receipt = assert_constraint(&owner, fragment, unit_node, source);
    assert!(receipt.arguments().is_empty());
    assert!(receipt.captured().request().arguments.is_empty());
    assert_eq!(receipt.captured().request().logical_argument_count, 0);
    let target = receipt.captured().binding().result_constraint().unwrap();
    assert!(!target.nullable());
    assert_eq!(target.logical_type(), ValueLogicalType::Physical);
    let DataType::List(item) = &target.data_type() else {
        panic!("typed ARRAY target");
    };
    assert_eq!(item.name(), "item");
    assert_eq!(item.data_type(), &DataType::Utf8);
    assert_eq!(
        target.child_at(0).unwrap().logical_type(),
        ValueLogicalType::Json
    );
    assert!(item.metadata().is_empty());
    let FunctionResultType::Scalar(selected) = &receipt.canonical_selection().unwrap().result_type
    else {
        panic!("array scalar selection");
    };
    let DataType::List(selected_item) = &selected.data_type() else {
        unreachable!();
    };
    assert!(Arc::ptr_eq(item, selected_item));
    assert_public_result_fields(&owner);
    let result = owner.plan().result_port().unwrap();
    assert_eq!(result.fields.len(), 1);
    assert_eq!(&result.fields[0].ty, target);
    assert_loan_prefixes(&owner, fragment, unit_node, source);
}
