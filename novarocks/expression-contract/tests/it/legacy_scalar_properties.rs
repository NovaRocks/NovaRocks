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

//! Existing scalar validation properties now exercise their neutral owner.
use super::support::*;
use arrow_schema::{DataType, Field};
use novarocks_expression_contract::*;
use novarocks_type_contract::{EvaluationDemand, FunctionArgumentType};
use std::sync::Arc;

#[test]
fn physical_nested_decimal_cast_preserves_its_frozen_mode() {
    let source = DataType::List(Arc::new(Field::new(
        "element",
        DataType::Decimal128(10, 2),
        true,
    )));
    let target = DataType::List(Arc::new(Field::new(
        "element",
        DataType::Decimal128(12, 3),
        true,
    )));
    for mode in [
        novarocks_type_contract::TrinoCastMode::Explicit,
        novarocks_type_contract::TrinoCastMode::Try,
        novarocks_type_contract::TrinoCastMode::Implicit,
    ] {
        let mut builder = BoundExprSetBuilder::new(limits());
        let input_type = ValueType::new(source.clone(), true);
        let (pools, reference) = null_source(&input_type);
        let operand = builder
            .add(None, input_type, ExprKind::Constant(reference))
            .unwrap();
        let expression = builder
            .add(
                None,
                ValueType::new(target.clone(), true),
                ExprKind::Cast {
                    cast_mode: mode,
                    expr: operand,
                    target: ValueType::new(target.clone(), true).data_type().clone(),
                },
            )
            .unwrap();
        builder
            .root(expression, EvaluationDemand::Value, RootScope::AllPorts)
            .unwrap();
        no_calls(builder, vec![], &Control::good()).unwrap();
        drop(pools);
    }
}
#[test]
fn lambda_parameter_type_is_part_of_its_declaration() {
    let mut builder = BoundExprSetBuilder::new(limits());
    let lambda = builder.reserve().unwrap();
    let utf8 = ValueType::new(DataType::Utf8, false);
    let parameter = builder
        .add(
            Some(lambda),
            utf8.clone(),
            ExprKind::LambdaParameter { lambda, ordinal: 0 },
        )
        .unwrap();
    builder
        .insert(
            lambda,
            None,
            utf8.clone(),
            ExprKind::Lambda {
                parameter_types: Box::new([integer(false)]),
                body: parameter,
            },
        )
        .unwrap();
    let function = function(
        "fixture/lambda/type",
        vec![FunctionArgumentType::Lambda {
            parameter_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::new([
                integer(false),
            ])),
            result_type: utf8.clone(),
        }],
        utf8.clone(),
    );
    let call = builder
        .add(
            None,
            utf8,
            ExprKind::FunctionCall {
                function,
                args: Box::new([lambda]),
            },
        )
        .unwrap();
    builder
        .root(call, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    let error = no_calls(builder, vec![], &Control::good())
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("lambda parameter does not reference a matching lambda owner"),
        "{error}"
    );
}
#[test]
fn wide_connective_is_width_while_below_arity_two_has_no_meaning() {
    for disjunction in [false, true] {
        for count in [0, 1, 512] {
            let mut builder = BoundExprSetBuilder::new(limits());
            let value = novarocks_constant_contract::ConstantValue::from_boolean(
                Arc::new(Field::new("source", DataType::Boolean, false)),
                boolean(false),
                true,
                policy(),
                novarocks_type_contract::CompilePhase::Validate,
                &Control::good(),
            )
            .unwrap();
            let reference = novarocks_constant_contract::ConstantReference {
                pool: novarocks_constant_contract::ConstantPoolId::new(4),
                ordinal: value.ordinal(),
            };
            let mut pools = novarocks_constant_contract::ConstantPools::empty();
            pools.insert(reference.pool, value.pool().clone()).unwrap();
            let args = (0..count)
                .map(|_| {
                    builder
                        .add(None, boolean(false), ExprKind::Constant(reference))
                        .unwrap()
                })
                .collect::<Vec<_>>();
            let expression = builder
                .add(
                    None,
                    boolean(false),
                    if disjunction {
                        ExprKind::Disjunction {
                            args: args.into_boxed_slice(),
                        }
                    } else {
                        ExprKind::Conjunction {
                            args: args.into_boxed_slice(),
                        }
                    },
                )
                .unwrap();
            builder
                .root(expression, EvaluationDemand::Value, RootScope::AllPorts)
                .unwrap();
            let result = no_calls(builder, vec![], &Control::good());
            if count >= 2 {
                result.unwrap();
            } else {
                assert!(
                    result
                        .unwrap_err()
                        .to_string()
                        .contains("boolean connective requires at least two arguments")
                );
            }
            drop(pools);
        }
    }
}

#[test]
fn original_257_cast_semantic_depth_is_a_resource_refusal_before_control_expansion() {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    let mut builder = BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: 256,
    });
    let (pools, reference) = i64_source(1);
    let mut definition = builder
        .add(None, integer(false), ExprKind::Constant(reference))
        .unwrap();
    for _ in 1..=256 {
        definition = builder
            .add(
                None,
                integer(false),
                ExprKind::Cast {
                    cast_mode: novarocks_type_contract::TrinoCastMode::Implicit,
                    expr: definition,
                    target: DataType::Int64,
                },
            )
            .unwrap();
    }
    builder
        .root(definition, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    let error = no_calls(builder, vec![], &Control::good()).unwrap_err();
    assert_eq!(error.failure_fact().class(), FailureClass::Resource);
    assert!(error.to_string().contains("semantic depth"));
    drop(pools);
}
