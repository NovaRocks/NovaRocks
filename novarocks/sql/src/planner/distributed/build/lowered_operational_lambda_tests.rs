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

use super::super::original_emission_test_observer::{
    OriginalEmissionObserver, OriginalEmissionView, lower_read_with_original_emission_observer,
};
use super::*;
use crate::planner::distributed::build::lowered_draft::SqlOperationalChannelRole;
use arrow::datatypes::Field;

fn array_map_source<'source>(
    view: &OriginalEmissionView<'source>,
) -> (
    FragmentId,
    &'source novarocks_physical_plan::PhysicalNode,
    &'source novarocks_expression_contract::ExprNode,
) {
    for (&fragment, builder) in view.fragments() {
        for (&node, actual) in builder.construction_nodes() {
            let Some(unit) = builder.construction_unit(node) else {
                continue;
            };
            if let Some(source) = unit.definitions().find(|source| {
                matches!(&source.kind,
                ContractExprKind::FunctionCall { function, .. }
                    if function.function_id.as_str() == "builtin.scalar/array_map/v1")
            }) {
                return (fragment, actual, source);
            }
        }
    }
    panic!("actual original ARRAY_MAP source with a Lambda argument")
}
struct Observe<F> {
    calls: usize,
    check: F,
}
impl<F> OriginalEmissionObserver for Observe<F>
where
    F: for<'source, 'control> FnMut(
        OriginalEmissionView<'source>,
        &mut CompileCheckpoints<'control>,
    ) -> Result<(), ContractLoweringError>,
{
    fn observe(
        &mut self,
        view: OriginalEmissionView<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), ContractLoweringError> {
        self.calls += 1;
        (self.check)(view, work)
    }
}
fn observe_original_lambda(
    expression: TypedExpr,
    check: impl for<'source, 'control> FnMut(
        OriginalEmissionView<'source>,
        &mut CompileCheckpoints<'control>,
    ) -> Result<(), ContractLoweringError>,
) {
    let plan = emission_plan(expression);
    let mut observer = Observe { calls: 0, check };
    let control = Control::default();
    let result = lower_read_with_original_emission_observer(
        &plan,
        version(),
        dop(),
        None,
        crate::functions::builtin_sql_function_catalog().snapshot(),
        false,
        policy(),
        &control,
        &mut observer,
    );
    assert_eq!(observer.calls, 1);
    assert!(matches!(result, Err(ContractLoweringError::FunctionBinding(
        novarocks_functions::FunctionBindingError::UnavailableImplementation(ref overload)
    )) if overload.as_str() == "builtin.scalar/array_map/dynamic-v1"));
}
fn original_operational_arguments(
    view: &OriginalEmissionView<'_>,
    receipt: &CheckedExpressionLogicalSourceEntry<'_>,
    control: &dyn PureCompileControl,
) -> Result<Box<[FunctionArgument]>, SqlOperationalProjectionError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let result = receipt.operational_arguments_observed(view.constants(), &mut work);
    if matches!(&result, Err(SqlOperationalProjectionError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn original_physical_constant(
    view: &OriginalEmissionView<'_>,
    receipt: &CheckedExpressionLogicalSourceEntry<'_>,
    id: ExprId,
) -> novarocks_functions::ConstantValue {
    let source = receipt.definitions().definition(id).unwrap();
    let ContractExprKind::Constant(reference) = source.kind else {
        panic!("actual emitted constant")
    };
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let result = view
        .constants()
        .resolve_source_observed(reference, &mut work)
        .unwrap();
    work.finish().unwrap();
    result
}

fn assert_lambda_projection(
    view: &OriginalEmissionView<'_>,
    unit_node: &novarocks_physical_plan::PhysicalNode,
    receipt: &CheckedExpressionLogicalSourceEntry<'_>,
    projected: &[FunctionArgument],
) {
    assert_eq!(receipt.arguments().len(), 2);
    assert_eq!(projected.len(), 2);
    assert_eq!(receipt.channels().len(), 2);
    assert!(matches!(
        receipt.channels()[0].role,
        SqlOperationalChannelRole::Lambda
    ));
    assert_eq!(receipt.channels()[0].expression, receipt.arguments()[0]);
    let original_request = receipt.captured().request();
    let FunctionArgument::Lambda {
        parameter_types: original,
        ..
    } = &original_request.arguments[0]
    else {
        panic!("original captured Lambda")
    };
    let lambda = receipt
        .definitions()
        .definition(receipt.arguments()[0])
        .unwrap();
    let ContractExprKind::Lambda {
        parameter_types,
        body,
    } = &lambda.kind
    else {
        panic!("actual emitted Lambda")
    };
    let body = receipt.definitions().definition(*body).unwrap();
    assert_eq!(
        receipt.definitions().owner(),
        crate::planner::distributed::build::unit_source::ScalarUnitOwner::Node(unit_node.id)
    );
    assert_eq!(lambda.lambda_scope, receipt.source().lambda_scope);
    assert!(std::ptr::eq(
        receipt.definitions().definition(body.id).unwrap(),
        body
    ));
    assert_eq!(body.lambda_scope, Some(lambda.id));
    assert_eq!(lambda.ty, body.ty);
    let FunctionArgument::Lambda {
        parameter_types: operational,
        result_type,
    } = &projected[0]
    else {
        panic!("operational Lambda")
    };
    assert_eq!(operational.as_ref(), original.as_ref());
    assert_eq!(operational.as_ref(), parameter_types.as_ref());
    assert_eq!(result_type, &body.ty);
    let FunctionArgument::Value {
        value_type,
        constant: projected_constant,
    } = &projected[1]
    else {
        panic!("operational source array Value")
    };
    let actual = receipt
        .definitions()
        .definition(receipt.arguments()[1])
        .unwrap();
    assert_eq!(value_type, &actual.ty);
    match (constant(&original_request.arguments[1]), projected_constant) {
        (None, None) => {
            assert!(matches!(
                receipt.channels()[1].role,
                SqlOperationalChannelRole::ValueWithoutConstant
            ));
        }
        (Some(original), Some(value)) => {
            assert!(matches!(
                receipt.channels()[1].role,
                SqlOperationalChannelRole::CapturedConstant
            ));
            let emitted = original_physical_constant(view, receipt, receipt.arguments()[1]);
            assert_eq!(value.ordinal(), original.ordinal());
            assert_eq!(value.ordinal(), emitted.ordinal());
            assert_eq!(
                value.pool().backing_identity(),
                original.pool().backing_identity()
            );
            assert_eq!(
                value.pool().backing_identity(),
                emitted.pool().backing_identity()
            );
            assert!(Arc::ptr_eq(
                value.pool().field_ref(),
                original.pool().field_ref()
            ));
            assert!(Arc::ptr_eq(
                value.pool().field_ref(),
                emitted.pool().field_ref()
            ));
        }
        _ => panic!("original array constant presence must be preserved"),
    }
}

#[test]
fn operational_lambda_analyzed_source_loans_original_parameters_and_actual_body_scope() {
    // Inspect the actual analyzer/emitter before mandatory capability admission.
    // ARRAY_MAP remains unsupported after the successful callback.
    observe_original_lambda(analyzed_lambda_expression_for_test(), |view, work| {
        let (fragment, unit_node, source) = array_map_source(&view);
        let receipt = view
            .checked_scalar_source(fragment, unit_node.id, source.id, work)
            .unwrap();
        // This is a repeated loan of the one original source, not a cloned
        // completed HOF owner. The supported LOWER owner clone contract remains
        // in scalar_journal_real_sql_retains_same_owner_binding_and_request_loans.
        let repeated = view
            .checked_scalar_source(fragment, unit_node.id, source.id, work)
            .unwrap();
        assert!(std::ptr::eq(receipt.captured(), repeated.captured()));
        assert!(std::ptr::eq(receipt.source(), source));
        assert_eq!(
            receipt.definitions().owner(),
            crate::planner::distributed::build::unit_source::ScalarUnitOwner::Node(unit_node.id)
        );
        assert!(std::ptr::eq(
            view.actual_node(fragment, unit_node.id).unwrap(),
            unit_node
        ));
        assert_eq!(receipt.captured().constant_policy(), policy());
        assert_eq!(
            receipt.captured().binding().decimal_overflow_policy(),
            DecimalOverflowPolicy::OutputNull
        );
        let projected =
            original_operational_arguments(&view, &receipt, &Control::default()).unwrap();
        assert_lambda_projection(&view, unit_node, &receipt, &projected);
        let FunctionArgument::Lambda {
            parameter_types,
            result_type,
        } = &projected[0]
        else {
            unreachable!()
        };
        assert_eq!(parameter_types.len(), 1);
        assert_eq!(parameter_types[0].data_type().clone(), DataType::Utf8);
        assert_eq!(result_type, &ValueType::new(DataType::Utf8, true));
        let lambda = receipt
            .definitions()
            .definition(receipt.arguments()[0])
            .unwrap();
        let ContractExprKind::Lambda { body, .. } = &lambda.kind else {
            unreachable!()
        };
        let body = receipt.definitions().definition(*body).unwrap();
        assert!(
            matches!(&body.kind, ContractExprKind::FunctionCall { function, .. }
        if function.function_id.as_str() == "builtin.scalar/lower/v1")
        );
        let body_receipt = view
            .checked_scalar_source(fragment, unit_node.id, body.id, work)
            .unwrap();
        assert!(constant(&body_receipt.captured().request().arguments[0]).is_none());
        Ok(())
    });
}

fn nested_identity_lambda_expression() -> (TypedExpr, ValueType) {
    // Honest already-typed emitter fixture, resolved by the actual ARRAY_MAP
    // catalog. No declaration is added and no nested runtime capability is
    // inferred from this generic source signature projection.
    let field = Arc::new(
        Field::new("provider_leaf", DataType::Utf8, false).with_metadata(
            std::collections::HashMap::<String, String>::from([(
                "provider.attribute".to_string(),
                "original-é".to_string(),
            )]),
        ),
    );
    assert_eq!(field.metadata()["provider.attribute"], "original-é");
    let parameter_type = ValueType::new(DataType::Struct(vec![field].into()), false);
    let lambda = TypedExpr {
        kind: ExprKind::LambdaFunction {
            params: vec![crate::analysis::LambdaParam {
                name: "item".into(),
                slot_id: 37,
                value_type: parameter_type.clone(),
            }],
            body: novarocks_parser::ast::AstBox::new(TypedExpr {
                kind: ExprKind::LambdaParamRef {
                    name: "item".into(),
                    slot_id: 37,
                },
                value_type: parameter_type.clone(),
            }),
        },
        value_type: ValueType::new(DataType::Null, true),
    };
    let array_type = ValueType::new(
        DataType::List(Arc::new(
            parameter_type.try_to_field("provider_item").unwrap(),
        )),
        true,
    );
    let array = TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Null),
        value_type: array_type,
    };
    let expressions = vec![lambda, array];
    let arguments = expressions
        .iter()
        .map(|expression| {
            crate::analysis::function_argument(expression, policy(), &Control::default()).unwrap()
        })
        .collect::<Vec<_>>();
    let resolved = crate::functions::builtin_sql_function_catalog()
        .resolve_scalar_binding("array_map", &arguments, &Control::default())
        .unwrap();
    let FunctionResultType::Scalar(result) = &resolved.selected.result_type else {
        panic!("scalar ARRAY_MAP")
    };
    let value_type = result.clone();
    let volatility = resolved.semantics.volatility;
    let expression = TypedExpr {
        kind: ExprKind::FunctionCall {
            name: "array_map".into(),
            args: (expressions).into(),
            distinct: false,
            binding: SqlFunctionBinding::new(resolved, DecimalOverflowPolicy::ReportError),
            volatility,
        },
        value_type,
    };
    (expression, parameter_type)
}

#[test]
fn operational_lambda_actual_catalog_typed_emitter_preserves_nested_parameter_metadata() {
    let (expression, expected) = nested_identity_lambda_expression();
    observe_original_lambda(expression, |view, work| {
        let (fragment, unit_node, source) = array_map_source(&view);
        let receipt = view
            .checked_scalar_source(fragment, unit_node.id, source.id, work)
            .unwrap();
        let projected =
            original_operational_arguments(&view, &receipt, &Control::default()).unwrap();
        assert_lambda_projection(&view, unit_node, &receipt, &projected);
        let FunctionArgument::Lambda {
            parameter_types,
            result_type,
        } = &projected[0]
        else {
            unreachable!()
        };
        assert_eq!(parameter_types.as_ref(), std::slice::from_ref(&expected));
        assert_eq!(result_type, &expected);
        let DataType::Struct(fields) = &parameter_types[0].data_type() else {
            unreachable!()
        };
        assert_eq!(fields[0].name(), "provider_leaf");
        assert!(fields[0].metadata().is_empty());
        assert_eq!(
            parameter_types[0].child_at(0).unwrap().logical_type(),
            novarocks_type_contract::ValueLogicalType::Physical
        );
        assert!(!fields[0].is_nullable());
        assert_eq!(
            receipt.captured().binding().decimal_overflow_policy(),
            DecimalOverflowPolicy::ReportError
        );
        Ok(())
    });
}

#[test]
fn operational_lambda_projector_preserves_every_actual_small_control_prefix() {
    observe_original_lambda(analyzed_lambda_expression_for_test(), |view, work| {
        let (fragment, unit_node, source) = array_map_source(&view);
        let receipt = view
            .checked_scalar_source(fragment, unit_node.id, source.id, work)
            .unwrap();
        // The real projection and its caller footer alone enter this trace.
        let baseline_control = Control::default();
        let projected = original_operational_arguments(&view, &receipt, &baseline_control).unwrap();
        assert_lambda_projection(&view, unit_node, &receipt, &projected);
        let baseline = baseline_control.trace();
        assert!(baseline.len() > 2);
        assert_eq!(baseline[0], (CompilePhase::FunctionSpecialization, 0));
        assert!(
            baseline
                .iter()
                .all(|(phase, _)| *phase == CompilePhase::FunctionSpecialization)
        );
        assert_eq!(baseline.last().unwrap().1, 0);
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
                    matches!(original_operational_arguments(&view, &receipt, &control),
                Err(SqlOperationalProjectionError::Control(actual)) if actual == cause)
                );
                assert_eq!(control.trace(), baseline[..=stop]);
            }
        }
        Ok(())
    });
}
