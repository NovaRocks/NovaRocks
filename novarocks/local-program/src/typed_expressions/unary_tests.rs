// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;

fn unary(
    kind: StaticExprKind,
    operand: (StaticExprNode, FunctionArgumentType),
    output: FunctionValueType,
    demand: EvaluationDemand,
) -> Result<ProgramTypedExpressions, ProgramExpressionTypeError> {
    let (node, ty) = operand;
    let (calls, types) = assembled(
        vec![node, StaticExprNode::new_value_typed(kind, output.clone())],
        vec![ty, FunctionArgumentType::Value(output.clone())],
        ProgramExprId::new(1),
        output,
        demand,
    );
    ProgramTypedExpressions::try_new(calls, types, &Control)
}

#[test]
fn public_unary_type_author_preserves_value_child_under_both_root_demands() {
    for demand in [EvaluationDemand::Value, EvaluationDemand::TruthOnly] {
        for (kind, nullable) in [
            (StaticExprKind::Not(ProgramExprId::new(0)), true),
            (StaticExprKind::IsNull(ProgramExprId::new(0)), false),
            (StaticExprKind::IsNotNull(ProgramExprId::new(0)), false),
        ] {
            let typed = unary(
                kind,
                boolean(None, true),
                FunctionValueType::new(DataType::Boolean, nullable),
                demand,
            )
            .unwrap();
            let flow = &typed.resolved_calls().snapshot().flows()[&ProgramExpressionArena::Main];
            let root = &flow.uses()[&ExpressionUseId::new(0)];
            assert_eq!(root.context.demand, demand);
            assert_eq!(
                flow.uses()[&root.arguments[0]].context.demand,
                EvaluationDemand::Value
            );
        }
        for json in [false, true] {
            unary(
                StaticExprKind::IsNull(ProgramExprId::new(0)),
                non_boolean(json),
                FunctionValueType::new(DataType::Boolean, false),
                demand,
            )
            .unwrap();
        }
    }
}

#[test]
fn public_unary_type_author_refuses_wrong_boolean_domains_and_nullability() {
    for demand in [EvaluationDemand::Value, EvaluationDemand::TruthOnly] {
        for json in [false, true] {
            // Numeric physical NOT is supported; raw text and nominal JSON are not.
            let operand_type = if json {
                FunctionValueType::try_with_logical_type(
                    DataType::Utf8,
                    false,
                    ValueLogicalType::Json,
                )
                .unwrap()
            } else {
                FunctionValueType::new(DataType::Utf8, false)
            };
            let field = Arc::new(operand_type.try_to_field("actual_unsupported_not").unwrap());
            let constant = ConstantValue::from_utf8(
                field,
                operand_type.clone(),
                "{}",
                policy(),
                CompilePhase::LowerProgram,
                &Control,
            )
            .unwrap();
            let operand = (
                StaticExprNode::new_value_typed(
                    StaticExprKind::Constant(constant),
                    novarocks_functions::FunctionValueType::new(DataType::Utf8, false),
                ),
                FunctionArgumentType::Value(operand_type),
            );
            assert_eq!(
                unary(
                    StaticExprKind::Not(ProgramExprId::new(0)),
                    operand,
                    FunctionValueType::new(DataType::Boolean, true),
                    demand
                )
                .unwrap_err(),
                ProgramExpressionTypeError::TypeMismatch
            );
        }
        assert_eq!(
            unary(
                StaticExprKind::Not(ProgramExprId::new(0)),
                boolean(None, true),
                FunctionValueType::new(DataType::Boolean, false),
                demand
            )
            .unwrap_err(),
            ProgramExpressionTypeError::TypeMismatch
        );
        for kind in [
            StaticExprKind::IsNull(ProgramExprId::new(0)),
            StaticExprKind::IsNotNull(ProgramExprId::new(0)),
        ] {
            assert_eq!(
                unary(
                    kind,
                    boolean(None, true),
                    FunctionValueType::new(DataType::Boolean, true),
                    demand
                )
                .unwrap_err(),
                ProgramExpressionTypeError::TypeMismatch
            );
        }
    }
    for kind in [
        StaticExprKind::Not(ProgramExprId::new(0)),
        StaticExprKind::IsNull(ProgramExprId::new(0)),
        StaticExprKind::IsNotNull(ProgramExprId::new(0)),
    ] {
        assert_eq!(
            unary(
                kind,
                boolean(Some(true), false),
                FunctionValueType::new(DataType::Int64, false),
                EvaluationDemand::Value
            )
            .unwrap_err(),
            ProgramExpressionTypeError::TypeMismatch
        );
    }
}

#[test]
fn public_null_predicate_rejects_callable_operand_without_erasing_its_value_result() {
    for negated in [false, true] {
        let (body, body_type) = boolean(Some(true), false);
        let output = FunctionValueType::new(DataType::Boolean, false);
        let (calls, types) = assembled(
            vec![
                body,
                StaticExprNode::new_value_typed(
                    StaticExprKind::SlotId(SlotId::new(1)),
                    novarocks_functions::FunctionValueType::new(DataType::Boolean, false),
                ),
                StaticExprNode::new_value_typed(
                    if negated {
                        StaticExprKind::IsNotNull(ProgramExprId::new(1))
                    } else {
                        StaticExprKind::IsNull(ProgramExprId::new(1))
                    },
                    novarocks_functions::FunctionValueType::new(DataType::Boolean, false),
                ),
            ],
            vec![
                body_type,
                FunctionArgumentType::Lambda {
                    parameter_types:
                        novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
                            Box::default(),
                        ),
                    result_type: output.clone(),
                },
                FunctionArgumentType::Value(output.clone()),
            ],
            ProgramExprId::new(2),
            output,
            EvaluationDemand::Value,
        );
        assert_eq!(
            ProgramTypedExpressions::try_new(calls, types, &Control).unwrap_err(),
            ProgramExpressionTypeError::WrongKind
        );
    }
}
