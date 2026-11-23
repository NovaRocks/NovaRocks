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
use crate::planner::payload::PlanRepeatNode;

fn reference(column: &OutputColumn) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: column.column_id,
            qualifier: None,
            column: column.name.clone(),
        },
        value_type: column.value_type.clone(),
    }
}
fn upper_call(argument: TypedExpr) -> TypedExpr {
    let args =
        [crate::analysis::function_argument(&argument, policy(), &Control::default()).unwrap()];
    let resolved = crate::functions::builtin_sql_function_catalog()
        .resolve_scalar_binding("upper", &args, &Control::default())
        .unwrap();
    let FunctionResultType::Scalar(result) = &resolved.selected.result_type else {
        panic!("actual UPPER scalar")
    };
    let value_type = result.clone();
    let volatility = resolved.semantics.volatility;
    TypedExpr {
        kind: ExprKind::FunctionCall {
            name: "upper".into(),
            args: (vec![argument]).into(),
            distinct: false,
            binding: SqlFunctionBinding::new(resolved, DecimalOverflowPolicy::ReportError),
            volatility,
        },
        value_type,
    }
}
pub(in crate::planner::distributed::build) fn repeat_plan(nested: bool) -> PhysicalPlanNode {
    // Already-typed source fixture, not a claim about postjoin SQL optimizer
    // placement. The real Repeat author creates a NullExtended value from a
    // nonnullable source; the original scalar binding predates that widening.
    let source = column(1, "source_text", DataType::Utf8, false);
    let grouping = column(2, "grouping", DataType::Int64, false);
    let mut repeated = source.clone();
    repeated.value_type = repeated.value_type.with_nullable(true).unwrap();
    let repeat = PhysicalPlanNode {
        kind: PhysicalPlanKind::Repeat(PlanRepeatNode {
            repeat_column_ref_list: vec![vec![source.name.clone()], vec![]],
            repeat_column_ref_ids: vec![vec![source.column_id], vec![]],
            grouping_ids: vec![0, 1],
            all_rollup_columns: vec![source.name.clone()],
            all_rollup_column_ids: vec![source.column_id],
            grouping_key_aliases: vec![],
            grouping_fn_args: vec![(grouping.name.clone(), vec![source.name.clone()])],
            grouping_fn_arg_ids: vec![vec![source.column_id]],
            grouping_fn_ids: vec![(grouping.name.clone(), grouping.column_id)],
            virtual_tuple_id: None,
        }),
        children: vec![values(vec![source.clone()], vec![vec![text("MiXeD-é")]])],
        output_columns: vec![repeated, grouping],
        stats: stats(),
        probe_runtime_filters: vec![],
    };
    let lower = lower_call(reference(&source), DecimalOverflowPolicy::ReportError);
    assert_eq!(lower.value_type, ValueType::new(DataType::Utf8, false));
    let expression = if nested { upper_call(lower) } else { lower };
    let mut project = emission_plan(expression);
    project.children = vec![repeat];
    project
}
pub(in crate::planner::distributed::build) fn finish(
    plan: &PhysicalPlanNode,
) -> SqlAuthoredPhysicalPlan {
    let control = Control::default();
    lower_draft_with_control(plan, &control)
        .unwrap()
        .finish_observed(&control)
        .unwrap()
}
pub(in crate::planner::distributed::build) fn lower_draft_with_control(
    plan: &PhysicalPlanNode,
    control: &dyn PureCompileControl,
) -> Result<LoweredSqlPhysicalDraft, ContractLoweringError> {
    lower_final_physical_plan(
        plan,
        version(),
        dop(),
        crate::functions::builtin_sql_function_catalog().snapshot(),
        novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
        false,
        policy(),
        control,
    )
}

fn source_named<'a>(
    owner: &'a SqlAuthoredPhysicalPlan,
    name: &str,
) -> (
    &'a Fragment,
    &'a novarocks_physical_plan::PhysicalNode,
    &'a novarocks_expression_contract::ExprNode,
) {
    let id = format!("builtin.scalar/{name}/v1");
    owner
        .plan()
        .fragments()
        .values()
        .find_map(|fragment| {
            fragment.nodes().values().find_map(|node| {
                node.expressions
                    .as_ref()?
                    .definitions()
                    .iter()
                    .find_map(|source| match &source.kind {
                        ContractExprKind::FunctionCall { function, .. }
                            if function.function_id.as_str() == id =>
                        {
                            Some((fragment, node, source))
                        }
                        _ => None,
                    })
            })
        })
        .expect("actual scalar source")
}
fn assert_canonical(
    owner: &SqlAuthoredPhysicalPlan,
    fragment: &Fragment,
    unit_node: &novarocks_physical_plan::PhysicalNode,
    source: &novarocks_expression_contract::ExprNode,
    expected: &ValueType,
) {
    let receipt = loan(owner, fragment, unit_node, source, &Control::default()).unwrap();
    let selected = receipt
        .canonical_selection()
        .expect("same-emission scalar canonical owner selection");
    let original = receipt.captured().binding().resolved();
    let ContractExprKind::FunctionCall { function, .. } = &source.kind else {
        unreachable!()
    };
    assert_eq!(function.function_id, original.function_id);
    assert_eq!(function.kind, original.kind);
    assert_eq!(function.overload, original.selected.overload);
    assert_eq!(function.overload, selected.overload);
    assert_eq!(function.argument_types, selected.argument_types);
    assert_eq!(&function.result_type, expected);
    assert_eq!(&source.ty, expected);
    assert_eq!(
        selected.result_type,
        FunctionResultType::Scalar(expected.clone())
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
    let clone = owner.clone();
    let clone_receipt = loan(&clone, fragment, unit_node, source, &Control::default()).unwrap();
    assert!(Arc::ptr_eq(
        selected,
        clone_receipt.canonical_selection().unwrap()
    ));
    assert!(std::ptr::eq(receipt.captured(), clone_receipt.captured()));
}
fn assert_result(
    owner: &SqlAuthoredPhysicalPlan,
    fragment: &Fragment,
    unit_node: &novarocks_physical_plan::PhysicalNode,
    source: &novarocks_expression_contract::ExprNode,
    expected: &ValueType,
) {
    let result = owner.plan().result_port().unwrap();
    assert_eq!(result.fields.len(), 1);
    let field = &result.fields[0];
    assert_eq!(&field.ty, expected);
    let result_fragment = owner.plan().fragments().get(&result.fragment).unwrap();
    let value = result_fragment.values().get(&field.value).unwrap();
    assert_eq!(&value.ty, expected);
    let source_value = if result.fragment == fragment.id() {
        value
    } else {
        // The actual SQL fixture gathers an exact source occurrence across
        // fragments. Check the real edge mapping rather than assuming that
        // the LOWER operator is in the client-result fragment.
        let ValueOrigin::ExchangeImport { edge, source_value } = value.origin else {
            panic!("actual gathered result origin");
        };
        let edge = owner.plan().edges().get(&edge).unwrap();
        assert_eq!(edge.source.fragment, fragment.id());
        assert_eq!(edge.destination.fragment, result.fragment);
        assert!(edge.source.projection.contains(&source_value));
        assert!(
            edge.destination
                .receive_mapping
                .contains(&(source_value, field.value))
        );
        fragment.values().get(&source_value).unwrap()
    };
    assert_eq!(&source_value.ty, expected);
    assert!(
        matches!(source_value.origin, ValueOrigin::Expr { node, root }
            if node == unit_node.id && std::ptr::eq(fragment.root_expression(node, root).unwrap(), source))
    );
    assert_eq!(result.output.columns.as_ref(), &[field.value]);
}

#[test]
fn canonical_scalar_repeat_child_nullable_is_authored_before_parent_and_result_publication() {
    let owner = finish(&repeat_plan(false));
    let (fragment, unit_node, source) = source_named(&owner, "lower");
    let receipt = loan(&owner, fragment, unit_node, source, &Control::default()).unwrap();
    let old = receipt.captured().request();
    assert_eq!(
        old.expected_result_type,
        Some(&ValueType::new(DataType::Utf8, false))
    );
    assert!(
        matches!(&old.arguments[0], FunctionArgument::Value { value_type, constant: None } if value_type == &ValueType::new(DataType::Utf8, false))
    );
    let child = receipt
        .definitions()
        .definition(receipt.arguments()[0])
        .unwrap();
    assert_eq!(child.ty, ValueType::new(DataType::Utf8, true));
    let ContractExprKind::Value(value) = child.kind else {
        panic!("actual materialized Repeat value")
    };
    let value = fragment.values().get(&value).unwrap();
    let ValueOrigin::NullExtended { node, of } = value.origin else {
        panic!("real Repeat NullExtended owner")
    };
    assert!(!fragment.values().get(&of).unwrap().ty.nullable());
    assert!(matches!(
        fragment.nodes().get(&node).unwrap().kind,
        NodeKind::Repeat { .. }
    ));
    let expected = ValueType::new(DataType::Utf8, true);
    assert_canonical(&owner, fragment, unit_node, source, &expected);
    assert_result(&owner, fragment, unit_node, source, &expected);
    assert_eq!(
        receipt.captured().binding().decimal_overflow_policy(),
        DecimalOverflowPolicy::ReportError
    );
    assert_eq!(receipt.captured().constant_policy(), policy());
}

#[test]
fn canonical_scalar_nested_upper_consumes_lower_actual_nullable_result_once() {
    let owner = finish(&repeat_plan(true));
    let (fragment, unit_node, lower) = source_named(&owner, "lower");
    let (upper_fragment, upper_node, upper) = source_named(&owner, "upper");
    assert!(std::ptr::eq(fragment, upper_fragment));
    assert!(std::ptr::eq(unit_node, upper_node));
    let expected = ValueType::new(DataType::Utf8, true);
    assert_canonical(&owner, fragment, unit_node, lower, &expected);
    assert_canonical(&owner, fragment, unit_node, upper, &expected);
    let receipt = loan(&owner, fragment, unit_node, upper, &Control::default()).unwrap();
    assert_eq!(receipt.arguments(), &[lower.id]);
    assert!(constant(&receipt.captured().request().arguments[0]).is_none());
    assert_eq!(
        receipt.captured().request().expected_result_type,
        Some(&ValueType::new(DataType::Utf8, false))
    );
    assert_result(&owner, fragment, unit_node, upper, &expected);
}

#[test]
fn canonical_scalar_original_none_survives_nested_and_identity_cast_constant_children() {
    for child in [
        TypedExpr {
            kind: ExprKind::Nested(novarocks_parser::ast::AstBox::new(text("ABC"))),
            value_type: ValueType::new(DataType::Utf8, false),
        },
        TypedExpr {
            kind: ExprKind::Cast {
                cast_mode: None,
                expr: novarocks_parser::ast::AstBox::new(text("ABC")),
                target: DataType::Utf8,
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            },
            value_type: ValueType::new(DataType::Utf8, false),
        },
    ] {
        let owner = authored(lower_call(child, DecimalOverflowPolicy::ReportError));
        let (fragment, unit_node, source) = source_named(&owner, "lower");
        let receipt = loan(&owner, fragment, unit_node, source, &Control::default()).unwrap();
        assert!(constant(&receipt.captured().request().arguments[0]).is_none());
        let projected = operational_arguments(&owner, &receipt, &Control::default()).unwrap();
        assert!(constant(&projected[0]).is_none());
        assert_eq!(
            physical_constant(&owner, &receipt, receipt.arguments()[0])
                .try_utf8()
                .unwrap(),
            Some("ABC")
        );
        let expected = ValueType::new(DataType::Utf8, false);
        assert_canonical(&owner, fragment, unit_node, source, &expected);
        assert_result(&owner, fragment, unit_node, source, &expected);
    }
}

#[test]
fn canonical_scalar_cv_null_and_real_sql_keep_original_source_distinct_from_selection() {
    let ty = ValueType::new(DataType::Utf8, true);
    let field = Arc::new(
        ty.try_to_field("original_selected_utf8")
            .unwrap()
            .with_metadata(std::collections::HashMap::<String, String>::from([(
                "provider.source".into(),
                "original-full-field".into(),
            )])),
    );
    let pool = ConstantPool::try_new(
        field.clone(),
        ty.clone(),
        StringArray::from(vec![Some("hidden"), Some("ΟΣİß"), None]).to_data(),
        policy(),
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap();
    let cv = pool.value(1).unwrap();
    let owner = authored(lower_call(
        TypedExpr {
            kind: ExprKind::Constant(cv.clone()),
            value_type: ty.clone(),
        },
        DecimalOverflowPolicy::ReportError,
    ));
    let (fragment, unit_node, source) = source_named(&owner, "lower");
    let receipt = loan(&owner, fragment, unit_node, source, &Control::default()).unwrap();
    let operational = operational_arguments(&owner, &receipt, &Control::default()).unwrap();
    let original_request = receipt.captured().request();
    for retained in [
        constant(&original_request.arguments[0]).unwrap(),
        constant(&operational[0]).unwrap(),
    ] {
        assert_eq!(retained.ordinal(), 1);
        assert_eq!(
            retained.pool().backing_identity(),
            cv.pool().backing_identity()
        );
        assert!(Arc::ptr_eq(retained.pool().field_ref(), &field));
    }
    assert_canonical(&owner, fragment, unit_node, source, &ty);
    assert_result(&owner, fragment, unit_node, source, &ty);

    let null = TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Null),
        value_type: ValueType::new(DataType::Null, true),
    };
    let owner = authored(lower_call(null, DecimalOverflowPolicy::ReportError));
    let (fragment, unit_node, source) = source_named(&owner, "lower");
    let receipt = loan(&owner, fragment, unit_node, source, &Control::default()).unwrap();
    let original = receipt.captured().request();
    let original_cv = constant(&original.arguments[0]).unwrap();
    assert_eq!(original_cv.value_type().data_type().clone(), DataType::Null);
    let operational = operational_arguments(&owner, &receipt, &Control::default()).unwrap();
    let typed_cv = constant(&operational[0]).unwrap();
    let ty = nullable_bounded_character(novarocks_type_contract::CharacterKind::Char, 0);
    assert_eq!(typed_cv.value_type(), &ty);
    assert_ne!(
        typed_cv.pool().backing_identity(),
        original_cv.pool().backing_identity()
    );
    let emitted = physical_constant(&owner, &receipt, receipt.arguments()[0]);
    assert_eq!(
        typed_cv.pool().backing_identity(),
        emitted.pool().backing_identity()
    );
    assert_canonical(&owner, fragment, unit_node, source, &ty);
    assert_result(&owner, fragment, unit_node, source, &ty);

    // Independent real analyzer/optimizer/provider completion, rather than a
    // claim that the typed Repeat fixture is a postjoin SQL optimizer route.
    let owner = crate::compiler::compile_authored_aggregate_for_test(
        "SELECT LOWER(CAST(order_key AS VARCHAR)) AS lower_key FROM orders",
    );
    let (fragment, unit_node, source) = source_named(&owner, "lower");
    let receipt = loan(&owner, fragment, unit_node, source, &Control::default()).unwrap();
    assert!(constant(&receipt.captured().request().arguments[0]).is_none());
    let selected = receipt.canonical_selection().unwrap();
    let FunctionResultType::Scalar(result) = &selected.result_type else {
        panic!("scalar selection")
    };
    assert_canonical(&owner, fragment, unit_node, source, result);
    assert_result(&owner, fragment, unit_node, source, result);
}

#[test]
fn canonical_scalar_actual_emission_preserves_each_success_and_ordinary_control_prefix() {
    let success = repeat_plan(true);
    let mut ordinary = repeat_plan(false);
    let PhysicalPlanKind::Project(project) = &mut ordinary.kind else {
        unreachable!()
    };
    let ExprKind::FunctionCall { args, .. } = &mut project.items[0].expr.kind else {
        unreachable!()
    };
    args.push(text("EXTRA"));
    let functions = crate::functions::builtin_sql_function_catalog().snapshot();
    for (plan, success) in [(success, true), (ordinary, false)] {
        let invoke = |control: &Control| {
            lower_final_physical_plan(
                &plan,
                version(),
                dop(),
                functions.clone(),
                novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
                false,
                policy(),
                control,
            )
        };
        let control = Control::default();
        assert_eq!(invoke(&control).is_ok(), success);
        let baseline = control.trace();
        assert_eq!(baseline[0].1, 0);
        if success {
            assert!(
                baseline
                    .iter()
                    .any(|(phase, _)| *phase == CompilePhase::FunctionSpecialization)
            );
        }
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
                assert!(
                    matches!(invoke(&control), Err(ContractLoweringError::Control(actual)) if actual == cause)
                );
                assert_eq!(control.trace(), baseline[..=stop]);
            }
        }
    }
}
