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

use super::support::*;
use arrow_schema::DataType;
use novarocks_expression_contract::*;
use novarocks_type_contract::*;
fn ty(data_type: DataType, nullable: bool) -> ValueType {
    ValueType::new(data_type, nullable)
}
#[derive(Clone, Copy)]
enum HigherOrderFixture {
    Valid,
    ScalarBinding,
    ParameterTypeDrift,
    ReusedAsExpressionChild,
    ReusedAsValueArgument,
    LambdaOperatorRoot,
}

fn higher_order_function_fragment(fixture: HigherOrderFixture) -> Result<BoundExprSet, UnitError> {
    let mut builder = BoundExprSetBuilder::new(limits());
    let lambda = builder.reserve().unwrap();
    let parameter_type = ty(DataType::Int64, false);
    let parameter = builder
        .add(
            Some(lambda),
            parameter_type.clone(),
            ExprKind::LambdaParameter { lambda, ordinal: 0 },
        )
        .unwrap();
    builder
        .insert(
            lambda,
            None,
            parameter_type.clone(),
            ExprKind::Lambda {
                parameter_types: Box::from([parameter_type.clone()]),
                body: parameter,
            },
        )
        .unwrap();
    let expected_argument = match fixture {
        HigherOrderFixture::Valid
        | HigherOrderFixture::ReusedAsExpressionChild
        | HigherOrderFixture::ReusedAsValueArgument
        | HigherOrderFixture::LambdaOperatorRoot => FunctionArgumentType::Lambda {
            parameter_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from([
                parameter_type.clone(),
            ])),
            result_type: parameter_type.clone(),
        },
        HigherOrderFixture::ScalarBinding => FunctionArgumentType::Value(parameter_type.clone()),
        HigherOrderFixture::ParameterTypeDrift => FunctionArgumentType::Lambda {
            parameter_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from([
                ty(DataType::Utf8, false),
            ])),
            result_type: parameter_type.clone(),
        },
    };
    let (argument_types, arguments): (Box<[FunctionArgumentType]>, Box<[ExprId]>) =
        if matches!(fixture, HigherOrderFixture::ReusedAsValueArgument) {
            (
                Box::from([
                    expected_argument,
                    FunctionArgumentType::Value(parameter_type.clone()),
                ]),
                Box::from([lambda, lambda]),
            )
        } else {
            (Box::from([expected_argument]), Box::from([lambda]))
        };
    let call = builder
        .add(
            None,
            parameter_type.clone(),
            ExprKind::FunctionCall {
                function: BoundFunction {
                    legacy_metadata: Some(LegacyBindingMetadata {
                        semantic_parameters: Box::default(),
                        volatility: FunctionVolatility::Immutable,
                        argument_evaluation: FunctionArgumentEvaluation::Eager,
                        failure_behavior: FunctionFailureBehavior::Propagate,
                        intrinsic_row_error:
                            novarocks_type_contract::FunctionIntrinsicRowError::NoRowError,
                    }),
                    function_id: FunctionId::try_new("builtin/test_higher_order/v1").unwrap(),
                    overload: FunctionOverloadId::try_new("lambda-i64-to-i64").unwrap(),
                    kind: FunctionKind::Scalar,
                    argument_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                        argument_types,
                    ),
                    result_type: parameter_type.clone(),
                },
                args: arguments,
            },
        )
        .unwrap();
    let result = if matches!(fixture, HigherOrderFixture::ReusedAsExpressionChild) {
        builder
            .add(
                None,
                parameter_type.clone(),
                ExprKind::Binary {
                    allow_throw_exception: Some(novarocks_type_contract::SemanticParameterRef {
                        id: novarocks_type_contract::SemanticParameterId::new(0),
                        expected_key:
                            novarocks_type_contract::SemanticParameterKey::AllowThrowException,
                    }),
                    decimal_overflow_policy:
                        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                    left: call,
                    op: BinaryOperator::Add,
                    right: lambda,
                },
            )
            .unwrap()
    } else {
        call
    };
    builder
        .root(result, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    if matches!(fixture, HigherOrderFixture::LambdaOperatorRoot) {
        builder
            .root(lambda, EvaluationDemand::Value, RootScope::AllPorts)
            .unwrap();
    }
    let f = match &builder.definition(call).unwrap().kind {
        ExprKind::FunctionCall { function, .. } => function,
        _ => unreachable!(),
    };
    let original = request(f);
    builder.request(call, original).unwrap();
    let control = ControlShape::HigherOrder {
        body_ordinal: 0,
        body_demand: EvaluationDemand::Value,
    };
    builder
        .definition_control(
            call,
            DefinitionControl {
                shape: control,
                temporal_sources: None,
            },
        )
        .unwrap();
    let caller = Control::good();
    let mut work = CompileCheckpoints::try_new(&caller, CompilePhase::Validate).unwrap();
    let result = builder.seal(vec![], &mut work).and_then(|sealed| {
        with_call_claims(
            sealed,
            ArgumentControl::HigherOrder {
                body_ordinal: 0,
                body_demand: EvaluationDemand::Value,
            },
            &mut work,
        )
    });
    if matches!(result, Err(UnitError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

#[test]
fn higher_order_function_binding_matches_the_exact_lambda_shape() {
    higher_order_function_fragment(HigherOrderFixture::Valid).unwrap();
    for fixture in [
        HigherOrderFixture::ScalarBinding,
        HigherOrderFixture::ParameterTypeDrift,
    ] {
        let error = higher_order_function_fragment(fixture)
            .unwrap_err()
            .to_string();
        let expected = match fixture {
            // The neutral unit rejects an actual lambda in a Value position
            // before deriving invocation control or checking the full call.
            HigherOrderFixture::ScalarBinding => {
                "lambda must appear only as an exact bound-function lambda argument"
            }
            HigherOrderFixture::ParameterTypeDrift => {
                "argument 0 shape differs from its bound signature"
            }
            _ => unreachable!(),
        };
        assert!(error.contains(expected), "{error}");
    }
}

#[test]
fn lambda_cannot_be_reused_outside_its_exact_bound_argument_positions() {
    for fixture in [
        HigherOrderFixture::ReusedAsExpressionChild,
        HigherOrderFixture::ReusedAsValueArgument,
        HigherOrderFixture::LambdaOperatorRoot,
    ] {
        let error = higher_order_function_fragment(fixture)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("lambda must appear only as an exact bound-function lambda argument")
        );
    }
}
