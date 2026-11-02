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

fn safe_fixture(
    dead: bool,
    left: FunctionValueType,
    right: FunctionValueType,
    result: FunctionValueType,
    demand: EvaluationDemand,
) -> (
    ProgramResolvedCalls,
    BTreeMap<ProgramExpressionArena, Vec<FunctionArgumentType>>,
) {
    let mut nodes = vec![
        slot(0, &left),
        slot(1, &right),
        StaticExprNode::new(
            StaticExprKind::PreparedNullSafeComparison {
                left: ProgramExprId::new(0),
                right: ProgramExprId::new(1),
            },
            result.data_type().clone(),
            None,
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
    assembled(nodes, types, root, root_type, demand)
}
fn safe_checked(
    dead: bool,
    left: FunctionValueType,
    right: FunctionValueType,
    result: FunctionValueType,
    demand: EvaluationDemand,
) -> Result<ProgramTypedExpressions, ProgramExpressionTypeError> {
    let (calls, types) = safe_fixture(dead, left, right, result, demand);
    ProgramTypedExpressions::try_new(calls, types, &Control)
}

#[test]
fn nullsafe_actual_and_dead_definitions_accept_nullable_inputs_but_require_nonnullable_boolean() {
    for dead in [false, true] {
        for demand in [EvaluationDemand::Value, EvaluationDemand::TruthOnly] {
            for (left, right) in [
                (integer(false), integer(false)),
                (integer(true), integer(false)),
                (integer(false), integer(true)),
                (integer(true), integer(true)),
                (
                    FunctionValueType::new(DataType::Null, false),
                    FunctionValueType::new(DataType::Null, false),
                ),
                (
                    FunctionValueType::new(DataType::Null, true),
                    FunctionValueType::new(DataType::Null, true),
                ),
            ] {
                let typed = safe_checked(dead, left.clone(), right.clone(), boolean(false), demand)
                    .unwrap();
                assert_eq!(
                    typed.definition_type(ProgramExpressionArena::Main, ProgramExprId::new(2)),
                    Some(&FunctionArgumentType::Value(boolean(false)))
                );
                let flow =
                    &typed.resolved_calls().snapshot().flows()[&ProgramExpressionArena::Main];
                if dead {
                    assert!(
                        flow.uses()
                            .values()
                            .all(|u| u.definition != ProgramExprId::new(2))
                    );
                } else {
                    let owner = &flow.uses()[&ExpressionUseId::new(0)];
                    assert_eq!(owner.context.demand, demand);
                    assert_eq!(owner.control, ControlShape::Eager);
                    assert_eq!(owner.arguments.len(), 2);
                    for (ordinal, child) in owner.arguments.iter().enumerate() {
                        assert_eq!(flow.uses()[child].definition, ProgramExprId::new(ordinal));
                        assert_eq!(flow.uses()[child].context.demand, EvaluationDemand::Value);
                    }
                }
                assert!(matches!(
                    safe_checked(dead, left, right, boolean(true), demand),
                    Err(ProgramExpressionTypeError::TypeMismatch)
                ));
            }
        }
    }
}

#[test]
fn nullsafe_rejects_wrong_result_and_exact_root_carrier_logical_precision_scale_unit_zone_drift() {
    let uuid = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::Uuid,
    )
    .unwrap();
    let json =
        FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
            .unwrap();
    let pairs = [
        (
            uuid,
            FunctionValueType::new(DataType::FixedSizeBinary(16), false),
        ),
        (json.clone(), FunctionValueType::new(DataType::Utf8, false)),
        (
            integer(false),
            FunctionValueType::new(DataType::Int32, false),
        ),
        (
            FunctionValueType::new(DataType::Decimal128(18, 2), false),
            FunctionValueType::new(DataType::Decimal128(17, 2), false),
        ),
        (
            FunctionValueType::new(DataType::Decimal128(18, 2), false),
            FunctionValueType::new(DataType::Decimal128(18, 3), false),
        ),
        (
            FunctionValueType::new(DataType::Timestamp(TimeUnit::Second, None), false),
            FunctionValueType::new(DataType::Timestamp(TimeUnit::Millisecond, None), false),
        ),
        (
            FunctionValueType::new(
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            FunctionValueType::new(
                DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
                false,
            ),
        ),
    ];
    for dead in [false, true] {
        for (left, right) in &pairs {
            assert!(matches!(
                safe_checked(
                    dead,
                    left.clone(),
                    right.clone(),
                    boolean(false),
                    EvaluationDemand::Value
                ),
                Err(ProgramExpressionTypeError::TypeMismatch)
            ));
        }
        for result in [integer(false), json.clone()] {
            assert!(matches!(
                safe_checked(
                    dead,
                    integer(false),
                    integer(false),
                    result,
                    EvaluationDemand::Value
                ),
                Err(ProgramExpressionTypeError::TypeMismatch)
            ));
        }
    }
}

#[test]
fn nullsafe_nested_static_identity_is_checked_without_claiming_runtime_capability() {
    let exact = nested(-91, "preserved", true, true);
    #[allow(deprecated)]
    let ordered_dictionary = |ordered| {
        FunctionValueType::new(
            DataType::Struct(
                vec![Field::new_dict(
                    "dict",
                    DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                    true,
                    7,
                    ordered,
                )]
                .into(),
            ),
            false,
        )
    };
    let unordered = ordered_dictionary(false);
    let ordered = ordered_dictionary(true);
    assert_eq!(
        unordered.data_type(),
        ordered.data_type(),
        "Arrow compatibility ignores IPC ordering"
    );
    for dead in [false, true] {
        assert!(
            safe_checked(
                dead,
                unordered.clone(),
                ordered.clone(),
                boolean(false),
                EvaluationDemand::Value
            )
            .is_ok()
        );

        assert!(
            safe_checked(
                dead,
                exact.clone(),
                exact.clone(),
                boolean(false),
                EvaluationDemand::Value
            )
            .is_ok()
        );
        for drift in [
            nested(-91, "preserved", false, true),
            nested(-91, "preserved", true, false),
        ] {
            assert!(matches!(
                safe_checked(
                    dead,
                    exact.clone(),
                    drift,
                    boolean(false),
                    EvaluationDemand::Value
                ),
                Err(ProgramExpressionTypeError::TypeMismatch)
            ));
        }
        for encoding_only in [
            nested(0, "preserved", true, true),
            nested(-91, "changed", true, true),
        ] {
            assert!(
                safe_checked(
                    dead,
                    exact.clone(),
                    encoding_only,
                    boolean(false),
                    EvaluationDemand::Value
                )
                .is_ok()
            );
        }
        // Complete types may be coherent while the old null-safe execution leaf
        // lacks these carriers. Static admission must not import its whitelist.
        let largeint = FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            false,
            ValueLogicalType::LargeInt,
        )
        .unwrap();
        assert!(
            safe_checked(
                dead,
                largeint.clone(),
                largeint,
                boolean(false),
                EvaluationDemand::Value
            )
            .is_ok()
        );
    }
}

#[test]
fn nullsafe_callable_operand_is_rejected_even_without_an_evaluation_use() {
    for dead in [false, true] {
        let ty = integer(false);
        let mut nodes = vec![
            slot(0, &ty),
            StaticExprNode::new(
                StaticExprKind::LambdaFunction {
                    body: ProgramExprId::new(0),
                    arg_slots: vec![],
                    common_sub_exprs: vec![],
                    is_nondeterministic: false,
                },
                DataType::Int64,
                None,
            ),
            slot(2, &ty),
            StaticExprNode::new(
                StaticExprKind::PreparedNullSafeComparison {
                    left: ProgramExprId::new(1),
                    right: ProgramExprId::new(2),
                },
                DataType::Boolean,
                None,
            ),
        ];
        let mut types = vec![
            FunctionArgumentType::Value(ty.clone()),
            FunctionArgumentType::Lambda {
                parameter_types: novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
                    Box::default(),
                ),
                result_type: ty.clone(),
            },
            FunctionArgumentType::Value(ty),
            FunctionArgumentType::Value(boolean(false)),
        ];
        let root = if dead {
            nodes.push(slot(4, &boolean(false)));
            types.push(FunctionArgumentType::Value(boolean(false)));
            ProgramExprId::new(4)
        } else {
            ProgramExprId::new(3)
        };
        let (calls, types) = assembled(nodes, types, root, boolean(false), EvaluationDemand::Value);
        assert!(matches!(
            ProgramTypedExpressions::try_new(calls, types, &Control),
            Err(ProgramExpressionTypeError::WrongKind)
        ));
    }
}

#[test]
fn nullsafe_wide_exact_metadata_walk_preserves_every_three_cause_callback_prefix_without_retry() {
    let wide = FunctionValueType::new(
        DataType::Struct(
            (0..320)
                .map(|i| {
                    Field::new(format!("f{i}"), DataType::Int64, false).with_metadata(
                        std::collections::HashMap::<String, String>::from([(
                            "provider.id".into(),
                            i.to_string(),
                        )]),
                    )
                })
                .collect(),
        ),
        false,
    );
    let (calls, types) = safe_fixture(
        false,
        wide.clone(),
        wide,
        boolean(false),
        EvaluationDemand::Value,
    );
    let success = Trace {
        at: None,
        cause: CompileControlError::Cancelled,
        seen: Mutex::default(),
        refused: Mutex::new(false),
    };
    ProgramTypedExpressions::try_new(calls.clone(), types.clone(), &success).unwrap();
    let expected = success.seen.into_inner().unwrap();
    assert!(expected.iter().any(|(_, units)| *units == 256));
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 0..expected.len() {
            let control = Trace {
                at: Some(at),
                cause: cause.clone(),
                seen: Mutex::default(),
                refused: Mutex::new(false),
            };
            assert!(
                matches!(ProgramTypedExpressions::try_new(calls.clone(),types.clone(),&control),Err(ProgramExpressionTypeError::Control(actual)) if actual==cause)
            );
            assert_eq!(*control.seen.lock().unwrap(), expected[..=at]);
        }
    }
}

#[test]
fn nullsafe_ordinary_invalid_definition_tail_preserves_typed_control_without_retry() {
    let (calls, types) = safe_fixture(
        false,
        integer(false),
        integer(false),
        boolean(true),
        EvaluationDemand::Value,
    );
    let success = Trace {
        at: None,
        cause: CompileControlError::Cancelled,
        seen: Mutex::default(),
        refused: Mutex::new(false),
    };
    assert!(matches!(
        ProgramTypedExpressions::try_new(calls.clone(), types.clone(), &success),
        Err(ProgramExpressionTypeError::TypeMismatch)
    ));
    let trace = success.seen.into_inner().unwrap();
    assert!(trace.len() > 1);
    assert!(trace.iter().all(|(_, units)| *units < 256));
    // The actual ordinary refusal still publishes completed progress. Refusing
    // this tail must preserve the primary control category, not the TypeMismatch.
    assert!(trace.last().is_some_and(|(_, units)| *units > 0));
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let control = Trace {
            at: Some(trace.len() - 1),
            cause: cause.clone(),
            seen: Mutex::default(),
            refused: Mutex::new(false),
        };
        assert!(
            matches!(ProgramTypedExpressions::try_new(calls.clone(),types.clone(),&control),Err(ProgramExpressionTypeError::Control(actual)) if actual==cause)
        );
        assert_eq!(*control.seen.lock().unwrap(), trace);
    }
}
