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
use novarocks_functions::CastOperation;
use novarocks_type_contract::DecimalOverflowPolicy;

fn cast_fixture(
    source: FunctionValueType,
    result: FunctionValueType,
    dead: bool,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> (
    ProgramResolvedCalls,
    BTreeMap<ProgramExpressionArena, Vec<FunctionArgumentType>>,
) {
    let root = if dead { 2 } else { 1 };
    let root_type = if dead { boolean(false) } else { result.clone() };
    assembled(
        vec![
            slot(0, &source),
            StaticExprNode::new_value_typed(
                StaticExprKind::PreparedCast {
                    operation: CastOperation::Carrier,
                    child: ProgramExprId::new(0),
                    decimal_overflow_policy: policy,
                    allow_throw_exception: allow,
                },
                result.clone(),
            ),
            slot(2, &boolean(false)),
        ],
        vec![
            FunctionArgumentType::Value(source),
            FunctionArgumentType::Value(result),
            FunctionArgumentType::Value(boolean(false)),
        ],
        ProgramExprId::new(root),
        root_type,
        EvaluationDemand::Value,
    )
}

#[test]
fn bool_cast_thirteen_profiles_check_actual_and_dead_full_types_and_frozen_policies() {
    let mut profiles = vec![(DataType::Boolean, DataType::Boolean)];
    for numeric in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
    ] {
        profiles.push((DataType::Boolean, numeric.clone()));
        profiles.push((numeric, DataType::Boolean));
    }
    assert_eq!(profiles.len(), 13);
    for (source_carrier, result_carrier) in profiles {
        for source_nullable in [false, true] {
            for result_nullable in [false, true] {
                for dead in [false, true] {
                    for allow in [false, true] {
                        for policy in [
                            DecimalOverflowPolicy::OutputNull,
                            DecimalOverflowPolicy::ReportError,
                        ] {
                            let source =
                                FunctionValueType::new(source_carrier.clone(), source_nullable);
                            let result =
                                FunctionValueType::new(result_carrier.clone(), result_nullable);
                            let (calls, types) =
                                cast_fixture(source.clone(), result.clone(), dead, policy, allow);
                            let typed = ProgramTypedExpressions::try_new(calls, types, &Control);
                            if source_nullable && !result_nullable {
                                assert_eq!(
                                    typed.unwrap_err(),
                                    ProgramExpressionTypeError::TypeMismatch,
                                );
                                continue;
                            }
                            let typed = typed.unwrap();
                            let arena = ProgramExpressionArena::Main;
                            assert_eq!(
                                typed.definition_type(arena, ProgramExprId::new(0)),
                                Some(&FunctionArgumentType::Value(source))
                            );
                            assert_eq!(
                                typed.definition_type(arena, ProgramExprId::new(1)),
                                Some(&FunctionArgumentType::Value(result))
                            );
                            let snapshot = typed.resolved_calls().snapshot();
                            assert!(matches!(
                                snapshot.roots().arenas()[&arena].node(ProgramExprId::new(1)).unwrap().kind(),
                                StaticExprKind::PreparedCast {
                                    operation: CastOperation::Carrier,
                                    child,
                                    decimal_overflow_policy,
                                    allow_throw_exception,
                                } if *child == ProgramExprId::new(0)
                                    && *decimal_overflow_policy == policy
                                    && *allow_throw_exception == allow
                            ));
                            let uses = snapshot.flows()[&arena].uses();
                            assert_eq!(uses.len(), if dead { 1 } else { 2 });
                            if !dead {
                                let child_use = &uses[&ExpressionUseId::new(1)];
                                assert_eq!(child_use.definition, ProgramExprId::new(0));
                                assert_eq!(child_use.context.demand, EvaluationDemand::Value);
                            }
                            // This proves the typed author, including dead definitions;
                            // it does not instantiate a runtime Boolean cast recipe.
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn bool_cast_all_definitions_reject_nominal_retagging_and_stale_carrier_facts() {
    let nominal = |carrier, logical| {
        FunctionValueType::try_with_logical_type(carrier, false, logical).unwrap()
    };
    for dead in [false, true] {
        for (source, result) in [
            (
                nominal(DataType::FixedSizeBinary(16), ValueLogicalType::LargeInt),
                boolean(true),
            ),
            (
                nominal(DataType::FixedSizeBinary(16), ValueLogicalType::Uuid),
                boolean(true),
            ),
            (
                nominal(DataType::Utf8, ValueLogicalType::Json),
                boolean(true),
            ),
            (
                boolean(false),
                nominal(DataType::FixedSizeBinary(16), ValueLogicalType::LargeInt),
            ),
            (
                boolean(false),
                nominal(DataType::FixedSizeBinary(16), ValueLogicalType::Uuid),
            ),
        ] {
            let (calls, types) = cast_fixture(
                source,
                result,
                dead,
                DecimalOverflowPolicy::OutputNull,
                false,
            );
            assert_eq!(
                ProgramTypedExpressions::try_new(calls, types, &Control).unwrap_err(),
                ProgramExpressionTypeError::TypeMismatch,
            );
        }
        // The retained static source is Boolean; the supplied complete source
        // claims Int64. Equal result facts cannot repair that stale source.
        let (calls, mut types) = cast_fixture(
            boolean(false),
            boolean(false),
            dead,
            DecimalOverflowPolicy::ReportError,
            true,
        );
        types.get_mut(&ProgramExpressionArena::Main).unwrap()[0] =
            FunctionArgumentType::Value(integer(false));
        assert_eq!(
            ProgramTypedExpressions::try_new(calls, types, &Control).unwrap_err(),
            ProgramExpressionTypeError::TypeMismatch
        );
        // Conversely the static cast says Boolean, while its supplied result
        // claims Int64. Both actual and dead definitions must be checked.
        let (calls, mut types) = cast_fixture(
            boolean(false),
            boolean(false),
            dead,
            DecimalOverflowPolicy::OutputNull,
            false,
        );
        types.get_mut(&ProgramExpressionArena::Main).unwrap()[1] =
            FunctionArgumentType::Value(integer(false));
        assert_eq!(
            ProgramTypedExpressions::try_new(calls, types, &Control).unwrap_err(),
            ProgramExpressionTypeError::TypeMismatch
        );
    }
}

#[test]
fn bool_cast_type_author_preserves_original_control_on_success_and_ordinary_failure_tails() {
    for dead in [false, true] {
        for valid in [false, true] {
            let (calls, types) = cast_fixture(
                boolean(true),
                FunctionValueType::new(DataType::Int64, valid),
                dead,
                DecimalOverflowPolicy::ReportError,
                true,
            );
            let recorder = Trace {
                at: None,
                cause: CompileControlError::Cancelled,
                seen: Mutex::new(vec![]),
                refused: Mutex::new(false),
            };
            let result = ProgramTypedExpressions::try_new(calls.clone(), types.clone(), &recorder);
            assert_eq!(result.is_ok(), valid);
            let expected = recorder.seen.into_inner().unwrap();
            assert_eq!(expected.first().unwrap().1, 0);
            assert!(expected.last().unwrap().1 > 0);
            for at in 0..expected.len() {
                for cause in [
                    CompileControlError::Cancelled,
                    CompileControlError::DeadlineExceeded,
                    CompileControlError::ResourceExhausted,
                ] {
                    let control = Trace {
                        at: Some(at),
                        cause: cause.clone(),
                        seen: Mutex::new(vec![]),
                        refused: Mutex::new(false),
                    };
                    assert!(matches!(
                        ProgramTypedExpressions::try_new(calls.clone(), types.clone(), &control),
                        Err(ProgramExpressionTypeError::Control(actual)) if actual == cause
                    ));
                    assert_eq!(*control.seen.lock().unwrap(), expected[..=at]);
                }
            }
        }
    }
}
