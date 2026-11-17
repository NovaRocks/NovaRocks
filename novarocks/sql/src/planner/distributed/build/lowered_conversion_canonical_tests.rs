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
use novarocks_type_contract::{NR_LOGICAL_TYPE_KEY, SemanticParameterKey, SemanticParameterValue};

fn assert_selection(
    captured: &crate::binding::CapturedLogicalCallArguments,
    selected: &novarocks_functions::FunctionBindingSelection,
    source: &novarocks_physical_plan::ExprNode,
) {
    let binding = captured.binding();
    let target = binding
        .result_constraint()
        .expect("actual intermediate producer target");
    assert_eq!(target, &ValueType::new(DataType::Utf8, false));
    assert_eq!(
        selected.result_type,
        FunctionResultType::Scalar(target.clone())
    );
    assert_eq!(&source.ty, target);
    let ContractExprKind::FunctionCall { function, args } = &source.kind else {
        unreachable!()
    };
    assert_eq!(function.function_id.as_str(), CONVERSION);
    assert_eq!(function.function_id, binding.resolved().function_id);
    assert_eq!(function.overload, binding.resolved().selected.overload);
    assert_eq!(function.overload, selected.overload);
    assert_eq!(function.argument_types, selected.argument_types);
    assert_eq!(&function.result_type, target);
    assert_eq!(args.len(), 1);
    assert!(selected.aggregate.is_none());
    assert!(
        function
            .legacy_metadata
            .as_ref()
            .expect("actual legacy fixture")
            .semantic_parameters
            .is_empty(),
        "conversion's original parameter shape"
    );
    assert!(matches!(&captured.request().arguments[0],
        FunctionArgument::Value { value_type, constant: None } if value_type == &json(false)));
    assert_eq!(captured.request().logical_argument_count, 1);
}

#[test]
fn canonical_conversion_constant_none_intermediate_target_and_following_cast_are_distinct() {
    for (final_type, decimal) in [
        (DataType::Utf8, DecimalOverflowPolicy::ReportError),
        (DataType::LargeUtf8, DecimalOverflowPolicy::OutputNull),
    ] {
        let input = selected_json();
        let ExprKind::Constant(original) = &input.kind else {
            unreachable!()
        };
        let final_type = ValueType::new(final_type, false);
        let owner = finish(cast(input.clone(), final_type.clone(), decimal));
        let (fragment, source) = conversion(&owner);
        let receipt = loan(&owner, fragment, source, &Control::default()).unwrap();
        let selected = receipt
            .canonical_selection()
            .expect("same-emission conversion selection");
        assert_selection(receipt.captured(), selected, source);
        assert_eq!(
            receipt.captured().binding().decimal_overflow_policy(),
            decimal
        );
        assert_eq!(receipt.captured().constant_policy(), policy());
        let actual = source_constant(&owner, fragment, receipt.arguments()[0]);
        assert_eq!(actual.ordinal(), 1);
        assert_eq!(actual.try_utf8().unwrap(), Some("{\"kept\":7}"));
        assert_eq!(actual.value_type(), &json(false));
        assert!(Arc::ptr_eq(actual.pool().array(), original.pool().array()));
        assert!(Arc::ptr_eq(
            actual.pool().field_ref(),
            original.pool().field_ref()
        ));
        assert_eq!(actual.pool().field_ref().name(), "original_json");
        assert_eq!(
            actual
                .pool()
                .field_ref()
                .metadata()
                .get(NR_LOGICAL_TYPE_KEY)
                .map(String::as_str),
            ValueLogicalType::Json.metadata_value()
        );
        let control = Control::default();
        let mut work =
            CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
        let operational = receipt
            .operational_arguments_observed(owner.plan().constants(), &mut work)
            .unwrap();
        work.finish().unwrap();
        assert!(
            matches!(&operational[0], FunctionArgument::Value { value_type, constant: None }
            if value_type == &json(false))
        );
        assert_eq!(
            selected.argument_types.as_ref(),
            operational
                .iter()
                .map(FunctionArgument::argument_type)
                .collect::<Vec<_>>()
                .as_slice()
        );
        let clone = owner.clone();
        let copied = loan(&clone, fragment, source, &Control::default()).unwrap();
        assert!(Arc::ptr_eq(selected, copied.canonical_selection().unwrap()));
        assert!(std::ptr::eq(receipt.captured(), copied.captured()));
        assert!(std::ptr::eq(
            receipt.captured().binding().result_constraint().unwrap(),
            copied.captured().binding().result_constraint().unwrap()
        ));
        let result = owner.plan().result_port().unwrap();
        assert_eq!(result.fields.len(), 1);
        assert_eq!(result.fields[0].ty, final_type);
        let result_fragment = owner.plan().fragments().get(&result.fragment).unwrap();
        assert_eq!(
            result_fragment
                .values()
                .get(&result.fields[0].value)
                .unwrap()
                .ty,
            final_type
        );
        if *final_type.data_type() == DataType::LargeUtf8 {
            let wrapper = fragment.expressions().iter().find_map(|(_, expression)|
                matches!(&expression.kind, ContractExprKind::Cast { expr, .. } if *expr == source.id)
                    .then_some(expression)).expect("separate same-domain carrier CAST");
            let ContractExprKind::Cast {
                target,
                decimal_overflow_policy,
                allow_throw_exception,
                ..
            } = &wrapper.kind
            else {
                unreachable!()
            };
            assert_eq!(target, &DataType::LargeUtf8);
            assert_eq!(*decimal_overflow_policy, decimal);
            assert_eq!(wrapper.ty, final_type);
            assert_ne!(wrapper.id, source.id);
            assert_eq!(
                allow_throw_exception.expected_key,
                SemanticParameterKey::AllowThrowException
            );
            // finish() explicitly supplies false to the original visitor;
            // this is that authored statement fact, not a session default.
            assert_eq!(
                owner.plan().parameters().get(allow_throw_exception.id),
                Some(&SemanticParameterValue::AllowThrowException(false))
            );
            assert!(matches!(
                loan(&owner, fragment, wrapper, &Control::default()),
                Err(SqlSourceJournalError::MissingEntry)
            ));
        } else {
            assert!(!fragment.expressions().iter().any(|(_, expression)|
                matches!(&expression.kind, ContractExprKind::Cast { expr, .. } if *expr == source.id)));
        }
    }
}

#[test]
fn canonical_conversion_raw_lambda_uses_original_lexical_scope_and_none_request() {
    // Actual direct visitor fixture. No complete fragment publication or
    // installed HOF/lifecycle capability is inferred from this lexical test.
    let input = TypedExpr {
        kind: ExprKind::LambdaParamRef {
            name: "json_arg".into(),
            slot_id: 11,
        },
        value_type: json(false),
    };
    let expression = TypedExpr {
        kind: ExprKind::LambdaFunction {
            params: vec![crate::analysis::LambdaParam {
                name: "json_arg".into(),
                slot_id: 11,
                value_type: json(false),
            }],
            body: novarocks_parser::ast::AstBox::new(cast(
                input,
                ValueType::new(DataType::Utf8, false),
                DecimalOverflowPolicy::ReportError,
            )),
        },
        value_type: ValueType::new(DataType::Null, true),
    };
    let control = Control::default();
    let mut visitor = ContractLoweringVisitor::new(
        version(),
        dop(),
        None,
        crate::functions::builtin_sql_function_catalog().snapshot(),
        false,
        policy(),
        &control,
    )
    .unwrap();
    let lambda = visitor
        .lower_expression(NodeId::new(73), &expression, &BTreeMap::new())
        .unwrap();
    assert_eq!(visitor.call_sources.expression_entries.len(), 1);
    let (_, entry) = visitor
        .call_sources
        .expression_entries
        .iter()
        .next()
        .unwrap();
    let fragment = &visitor.fragments[&ROOT_FRAGMENT_ID];
    let source=fragment.expressions().iter().find_map(|(_,node)|
        matches!(&node.kind,ContractExprKind::FunctionCall {function,..} if function.function_id.as_str()==CONVERSION)
            .then_some(node)).unwrap();
    let selected = entry
        .canonical_operational
        .as_ref()
        .expect("scoped conversion canonical receipt")
        .selected();
    assert_selection(entry.captured.captured(), selected, source);
    assert_eq!(source.owner, NodeId::new(73));
    assert_eq!(source.lambda_scope, Some(lambda));
    assert_eq!(entry.lambda_scope, Some(lambda));
    let ContractExprKind::Lambda {
        parameter_types,
        body,
    } = &fragment.expressions().get(lambda).unwrap().kind
    else {
        panic!("actual Lambda author");
    };
    assert_eq!(*body, source.id);
    let ContractExprKind::FunctionCall { args, .. } = &source.kind else {
        unreachable!()
    };
    assert_eq!(entry.arguments.as_ref(), args.as_ref());
    assert_eq!(parameter_types.as_ref(), &[json(false)]);
    let parameter = fragment.expressions().get(args[0]).unwrap();
    assert!(matches!(parameter.kind,
        ContractExprKind::LambdaParameter { lambda: scope, ordinal: 0 } if scope == lambda));
    assert_eq!(parameter.ty, json(false));
    assert_eq!(parameter.lambda_scope, Some(lambda));
    validate_expression_source_entry_observed(entry, source, &mut visitor.work).unwrap();
    visitor.work.finish().unwrap();
}
