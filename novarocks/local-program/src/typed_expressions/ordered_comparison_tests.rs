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
use novarocks_functions::ComparisonOperator;

const OPERATORS: [ComparisonOperator; 4] = [
    ComparisonOperator::Lt,
    ComparisonOperator::Le,
    ComparisonOperator::Gt,
    ComparisonOperator::Ge,
];

fn ordered(
    operator: ComparisonOperator,
    dead: bool,
    left: FunctionValueType,
    right: FunctionValueType,
    result: FunctionValueType,
    demand: EvaluationDemand,
) -> Result<ProgramTypedExpressions, ProgramExpressionTypeError> {
    let mut nodes = vec![
        slot(0, &left),
        slot(1, &right),
        StaticExprNode::new_value_typed(
            StaticExprKind::from_comparison(operator, ProgramExprId::new(0), ProgramExprId::new(1)),
            result.clone(),
        ),
    ];
    let mut types = vec![
        FunctionArgumentType::Value(left),
        FunctionArgumentType::Value(right),
        FunctionArgumentType::Value(result.clone()),
    ];
    let (root, root_type) = if dead {
        nodes.push(slot(3, &boolean(false)));
        types.push(FunctionArgumentType::Value(boolean(false)));
        (ProgramExprId::new(3), boolean(false))
    } else {
        (ProgramExprId::new(2), result)
    };
    let (calls, types) = assembled(nodes, types, root, root_type, demand);
    ProgramTypedExpressions::try_new(calls, types, &Control)
}

#[test]
fn ordered_actual_and_dead_definitions_keep_value_domains_and_root_null_admission() {
    for operator in OPERATORS {
        for dead in [false, true] {
            for demand in [EvaluationDemand::Value, EvaluationDemand::TruthOnly] {
                for (left, right) in [(false, false), (false, true), (true, false), (true, true)] {
                    ordered(
                        operator,
                        dead,
                        integer(left),
                        integer(right),
                        boolean(left || right),
                        demand,
                    )
                    .unwrap();
                    ordered(
                        operator,
                        dead,
                        integer(left),
                        integer(right),
                        boolean(true),
                        demand,
                    )
                    .unwrap();
                    if left || right {
                        assert_eq!(
                            ordered(
                                operator,
                                dead,
                                integer(left),
                                integer(right),
                                boolean(false),
                                demand
                            )
                            .unwrap_err(),
                            ProgramExpressionTypeError::TypeMismatch
                        );
                    }
                }
                let null = FunctionValueType::new(DataType::Null, false);
                ordered(
                    operator,
                    dead,
                    null.clone(),
                    null.clone(),
                    boolean(true),
                    demand,
                )
                .unwrap();
                assert_eq!(
                    ordered(operator, dead, null.clone(), null, boolean(false), demand)
                        .unwrap_err(),
                    ProgramExpressionTypeError::TypeMismatch
                );
            }
        }
    }
}

#[test]
fn ordered_static_identity_excludes_null_safe_and_rejects_cross_nominal_or_wrong_result() {
    assert!(
        StaticExprKind::PreparedNullSafeComparison {
            left: ProgramExprId::new(0),
            right: ProgramExprId::new(1)
        }
        .ordinary_comparison()
        .is_none()
    );
    for operator in OPERATORS {
        for dead in [false, true] {
            let physical = FunctionValueType::new(DataType::FixedSizeBinary(16), false);
            for logical in [ValueLogicalType::LargeInt, ValueLogicalType::Uuid] {
                let nominal = FunctionValueType::try_with_logical_type(
                    DataType::FixedSizeBinary(16),
                    false,
                    logical,
                )
                .unwrap();
                ordered(
                    operator,
                    dead,
                    nominal.clone(),
                    nominal.clone(),
                    boolean(false),
                    EvaluationDemand::Value,
                )
                .unwrap();
                assert_eq!(
                    ordered(
                        operator,
                        dead,
                        nominal,
                        physical.clone(),
                        boolean(false),
                        EvaluationDemand::Value
                    )
                    .unwrap_err(),
                    ProgramExpressionTypeError::TypeMismatch
                );
            }
            assert_eq!(
                ordered(
                    operator,
                    dead,
                    integer(false),
                    integer(false),
                    integer(false),
                    EvaluationDemand::Value
                )
                .unwrap_err(),
                ProgramExpressionTypeError::TypeMismatch
            );
            assert_eq!(
                ordered(
                    operator,
                    dead,
                    integer(false),
                    FunctionValueType::new(DataType::UInt64, false),
                    boolean(false),
                    EvaluationDemand::Value
                )
                .unwrap_err(),
                ProgramExpressionTypeError::TypeMismatch
            );
        }
    }
}
