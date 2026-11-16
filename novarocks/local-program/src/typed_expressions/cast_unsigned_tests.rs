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

fn fixture(
    source: FunctionValueType,
    result: FunctionValueType,
    dead: bool,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> (
    ProgramResolvedCalls,
    BTreeMap<ProgramExpressionArena, Vec<FunctionArgumentType>>,
) {
    let root_type = if dead { boolean(false) } else { result.clone() };
    assembled(
        vec![
            slot(0, &source),
            StaticExprNode::new(
                StaticExprKind::PreparedCast {
                    operation: CastOperation::Carrier,
                    child: ProgramExprId::new(0),
                    decimal_overflow_policy: policy,
                    allow_throw_exception: allow,
                    array_failure_mode: None,
                },
                result.data_type().clone(),
                None,
            ),
            slot(2, &boolean(false)),
        ],
        vec![
            FunctionArgumentType::Value(source),
            FunctionArgumentType::Value(result),
            FunctionArgumentType::Value(boolean(false)),
        ],
        ProgramExprId::new(if dead { 2 } else { 1 }),
        root_type,
        EvaluationDemand::Value,
    )
}

fn integer_width(ty: &DataType) -> Option<(bool, u8)> {
    Some(match ty {
        DataType::Int8 => (true, 8),
        DataType::Int16 => (true, 16),
        DataType::Int32 => (true, 32),
        DataType::Int64 => (true, 64),
        DataType::UInt8 => (false, 8),
        DataType::UInt16 => (false, 16),
        DataType::UInt32 => (false, 32),
        DataType::UInt64 => (false, 64),
        _ => return None,
    })
}

// Independent scalar range oracle: this does not call the production NULL
// classifier and does not authorize a runtime cast for a dead definition.
fn possible_conversion_null(source: &DataType, target: &DataType, allow: bool) -> bool {
    match (integer_width(source), integer_width(target)) {
        (Some((true, _)), Some((false, _))) => true,
        (Some((false, source)), Some((true, target))) => target <= source,
        (Some((false, source)), Some((false, target))) => target < source,
        (None, Some((false, _))) if matches!(source, DataType::Float32 | DataType::Float64) => {
            !allow
        }
        _ => false,
    }
}

#[test]
fn unsigned_cast_seventy_two_profiles_check_all_definition_nullability_and_frozen_sources() {
    let unsigned = [
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
    ];
    let existing = [
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
    ];
    let mut profiles = Vec::new();
    for source in &unsigned {
        for target in &existing {
            profiles.push((source.clone(), target.clone()));
            profiles.push((target.clone(), source.clone()));
        }
        for target in &unsigned {
            profiles.push((source.clone(), target.clone()));
        }
    }
    assert_eq!(profiles.len(), 72);
    for (source_carrier, result_carrier) in profiles {
        for source_nullable in [false, true] {
            for result_nullable in [false, true] {
                for allow in [false, true] {
                    let valid = result_nullable
                        || (!source_nullable
                            && !possible_conversion_null(&source_carrier, &result_carrier, allow));
                    for policy in [
                        DecimalOverflowPolicy::OutputNull,
                        DecimalOverflowPolicy::ReportError,
                    ] {
                        for dead in [false, true] {
                            let source =
                                FunctionValueType::new(source_carrier.clone(), source_nullable);
                            let result =
                                FunctionValueType::new(result_carrier.clone(), result_nullable);
                            let (calls, types) =
                                fixture(source.clone(), result.clone(), dead, policy, allow);
                            let typed = ProgramTypedExpressions::try_new(calls, types, &Control);
                            if !valid {
                                assert_eq!(
                                    typed.unwrap_err(),
                                    ProgramExpressionTypeError::TypeMismatch,
                                    "{source_carrier:?}->{result_carrier:?} source_nullable={source_nullable} result_nullable={result_nullable} allow={allow} dead={dead}"
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
                            assert!(
                                matches!(snapshot.roots().arenas()[&arena].node(ProgramExprId::new(1)).unwrap().kind(), StaticExprKind::PreparedCast { operation: CastOperation::Carrier, child, decimal_overflow_policy, allow_throw_exception, array_failure_mode: None } if *child == ProgramExprId::new(0) && *decimal_overflow_policy == policy && *allow_throw_exception == allow)
                            );
                            assert_eq!(
                                snapshot.flows()[&arena].uses().len(),
                                if dead { 1 } else { 2 }
                            );
                            // Source slots are accurate API type facts, not SQL
                            // UNSIGNED syntax, literal retagging or a provider proof.
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn unsigned_cast_actual_and_dead_definitions_reject_nominal_and_stale_type_facts() {
    let nominal = |logical| {
        FunctionValueType::try_with_logical_type(DataType::FixedSizeBinary(16), false, logical)
            .unwrap()
    };
    for dead in [false, true] {
        for logical in [ValueLogicalType::Uuid, ValueLogicalType::LargeInt] {
            for reverse in [false, true] {
                let unsigned = FunctionValueType::new(DataType::UInt64, false);
                let (source, result) = if reverse {
                    (unsigned, nominal(logical))
                } else {
                    (nominal(logical), unsigned)
                };
                let (calls, types) = fixture(
                    source,
                    result,
                    dead,
                    DecimalOverflowPolicy::ReportError,
                    true,
                );
                assert_eq!(
                    ProgramTypedExpressions::try_new(calls, types, &Control).unwrap_err(),
                    ProgramExpressionTypeError::TypeMismatch
                );
            }
        }
        let source = FunctionValueType::new(DataType::UInt8, false);
        for stale_index in [0, 1] {
            let (calls, mut types) = fixture(
                source.clone(),
                source.clone(),
                dead,
                DecimalOverflowPolicy::OutputNull,
                false,
            );
            types.get_mut(&ProgramExpressionArena::Main).unwrap()[stale_index] =
                FunctionArgumentType::Value(FunctionValueType::new(DataType::UInt64, false));
            assert_eq!(
                ProgramTypedExpressions::try_new(calls, types, &Control).unwrap_err(),
                ProgramExpressionTypeError::TypeMismatch
            );
        }
    }
}

#[test]
fn unsigned_cast_type_checks_preserve_original_control_at_success_and_error_tails() {
    for dead in [false, true] {
        for valid in [false, true] {
            // UInt32 fits Int64; UInt64 does not fit Int64 for all values.
            let source = FunctionValueType::new(
                if valid {
                    DataType::UInt32
                } else {
                    DataType::UInt64
                },
                false,
            );
            let (calls, types) = fixture(
                source,
                integer(false),
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
                    assert!(
                        matches!(ProgramTypedExpressions::try_new(calls.clone(), types.clone(), &control), Err(ProgramExpressionTypeError::Control(actual)) if actual == cause)
                    );
                    assert_eq!(*control.seen.lock().unwrap(), expected[..=at]);
                }
            }
        }
    }
}

#[test]
fn carrier_cast_cannot_erase_nested_nominal_domain_even_in_an_unused_definition() {
    let source = crate::test_value_types::string_structure(&[("payload", true, true)], true);
    let target = crate::test_value_types::string_structure(&[("payload", true, false)], true);
    assert_eq!(source.data_type(), target.data_type());
    for dead in [false, true] {
        let (calls, types) = fixture(
            source.clone(),
            target.clone(),
            dead,
            DecimalOverflowPolicy::OutputNull,
            false,
        );
        assert_eq!(
            ProgramTypedExpressions::try_new(calls, types, &Control).unwrap_err(),
            ProgramExpressionTypeError::TypeMismatch
        );
    }
}

fn null_json_list(
    item: novarocks_type_contract::ValueKind,
    carrier: DataType,
    item_nullable: bool,
    nullable: bool,
) -> FunctionValueType {
    use novarocks_type_contract::{ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime};
    let storage = ValueTypeStorageLifetime::pure();
    let child = ValueTypeRef::try_new(item, item_nullable, storage.clone()).unwrap();
    let layout = ValueLayout::try_new(
        &DataType::List(Arc::new(Field::new("item", carrier, item_nullable))),
        storage.clone(),
    )
    .unwrap();
    FunctionValueType::try_from_value(
        ValueTypeRef::try_new(ValueKind::List(child), nullable, storage).unwrap(),
        layout,
    )
    .unwrap()
}

fn null_json_fixture(
    source: FunctionValueType,
    result: FunctionValueType,
    operation: CastOperation,
    dead: bool,
    policy: DecimalOverflowPolicy,
    allow: bool,
    constant: Option<novarocks_functions::ConstantValue>,
) -> (
    ProgramResolvedCalls,
    BTreeMap<ProgramExpressionArena, Vec<FunctionArgumentType>>,
) {
    let root_type = if dead { boolean(false) } else { result.clone() };
    assembled(
        vec![
            StaticExprNode::new_value_typed(
                constant.map_or_else(
                    || StaticExprKind::SlotId(SlotId::new(1)),
                    StaticExprKind::Constant,
                ),
                source.clone(),
            ),
            StaticExprNode::new_value_typed(
                StaticExprKind::PreparedCast {
                    operation,
                    child: ProgramExprId::new(0),
                    decimal_overflow_policy: policy,
                    allow_throw_exception: allow,
                    array_failure_mode: None,
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
        ProgramExprId::new(if dead { 2 } else { 1 }),
        root_type,
        EvaluationDemand::Value,
    )
}

#[test]
fn null_list_json_carrier_cast_checks_live_and_dead_definitions_with_full_types() {
    use novarocks_type_contract::ValueKind;
    for (source_nullable, target_nullable) in [(false, false), (false, true), (true, true)] {
        let source = null_json_list(
            ValueKind::Scalar(DataType::Null),
            DataType::Null,
            true,
            source_nullable,
        );
        let target = null_json_list(
            ValueKind::Nominal(ValueLogicalType::Json),
            DataType::Utf8,
            true,
            target_nullable,
        );
        for dead in [false, true] {
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                for allow in [false, true] {
                    let (calls, types) = null_json_fixture(
                        source.clone(),
                        target.clone(),
                        CastOperation::Carrier,
                        dead,
                        policy,
                        allow,
                        None,
                    );
                    let typed = ProgramTypedExpressions::try_new(calls, types, &Control).unwrap();
                    let arena = ProgramExpressionArena::Main;
                    assert_eq!(
                        typed.definition_type(arena, ProgramExprId::new(0)),
                        Some(&FunctionArgumentType::Value(source.clone()))
                    );
                    assert_eq!(
                        typed.definition_type(arena, ProgramExprId::new(1)),
                        Some(&FunctionArgumentType::Value(target.clone()))
                    );
                    let snapshot = typed.resolved_calls().snapshot();
                    let cast = snapshot.roots().arenas()[&arena]
                        .node(ProgramExprId::new(1))
                        .unwrap();
                    assert_eq!(cast.value_type(), Some(&target));
                    assert!(
                        matches!(cast.kind(), StaticExprKind::PreparedCast { operation: CastOperation::Carrier, child, decimal_overflow_policy, allow_throw_exception, array_failure_mode: None } if *child == ProgramExprId::new(0) && *decimal_overflow_policy == policy && *allow_throw_exception == allow)
                    );
                    assert_eq!(
                        snapshot.flows()[&arena].uses().len(),
                        if dead { 1 } else { 2 }
                    );
                }
            }
        }
    }
}

#[test]
fn null_list_json_exception_refuses_required_targets_root_nulls_and_foreign_operations() {
    use novarocks_type_contract::ValueKind;
    let source = null_json_list(
        ValueKind::Scalar(DataType::Null),
        DataType::Null,
        true,
        false,
    );
    let target = null_json_list(
        ValueKind::Nominal(ValueLogicalType::Json),
        DataType::Utf8,
        true,
        true,
    );
    let required_item = null_json_list(
        ValueKind::Nominal(ValueLogicalType::Json),
        DataType::Utf8,
        false,
        true,
    );
    let required_root = null_json_list(
        ValueKind::Nominal(ValueLogicalType::Json),
        DataType::Utf8,
        true,
        false,
    );
    for dead in [false, true] {
        for (source, result, operation) in [
            (
                source.clone(),
                required_item.clone(),
                CastOperation::Carrier,
            ),
            (
                source.clone().with_nullable(true).unwrap(),
                required_root.clone(),
                CastOperation::Carrier,
            ),
            (
                FunctionValueType::new(DataType::Null, true),
                target.clone(),
                CastOperation::Carrier,
            ),
            (source.clone(), target.clone(), CastOperation::Time),
            (
                source.clone(),
                target.clone(),
                CastOperation::TimeFromDatetime,
            ),
        ] {
            let (calls, types) = null_json_fixture(
                source,
                result,
                operation,
                dead,
                DecimalOverflowPolicy::OutputNull,
                false,
                None,
            );
            assert_eq!(
                ProgramTypedExpressions::try_new(calls, types, &Control).unwrap_err(),
                ProgramExpressionTypeError::TypeMismatch
            );
        }
    }
}

#[test]
fn nonempty_utf8_list_does_not_acquire_json_domain_in_live_or_dead_cast() {
    use arrow_array::{
        Array,
        builder::{ListBuilder, StringBuilder},
    };
    use novarocks_type_contract::ValueKind;
    let text = "original nonempty UTF8";
    let mut builder = ListBuilder::with_capacity(StringBuilder::with_capacity(1, text.len()), 1);
    builder.values().append_value(text);
    builder.append(true);
    let array = builder.finish();
    let source = FunctionValueType::new(array.data_type().clone(), false);
    let field = Arc::new(Field::new(
        "original_utf8_list",
        array.data_type().clone(),
        false,
    ));
    let policy = novarocks_functions::ConstantPolicy {
        max_rows: 4,
        max_array_nodes: 16,
        max_logical_elements: 16,
        max_retained_buffer_bytes: 4096,
        max_type_depth: 8,
        max_type_nodes: 16,
        max_dictionary_depth: 8,
        max_metadata_bytes: 4096,
        max_library_validation_work: 4096,
        // The original nested Arrow validation envelope includes diagnostic
        // workspace (nodes * 256 * depth * 16), beyond retained input bytes.
        max_library_validation_bytes: 65536,
    };
    let pool = novarocks_functions::ConstantPool::try_new(
        field,
        source.clone(),
        array.to_data(),
        policy,
        CompilePhase::LowerProgram,
        &Control,
    )
    .unwrap();
    let original = pool.value(0).unwrap();
    let target = null_json_list(
        ValueKind::Nominal(ValueLogicalType::Json),
        DataType::Utf8,
        true,
        true,
    );
    assert_eq!(source.data_type(), target.data_type());
    for dead in [false, true] {
        let (calls, types) = null_json_fixture(
            source.clone(),
            target.clone(),
            CastOperation::Carrier,
            dead,
            DecimalOverflowPolicy::OutputNull,
            false,
            Some(original.clone()),
        );
        assert_eq!(
            ProgramTypedExpressions::try_new(calls, types, &Control).unwrap_err(),
            ProgramExpressionTypeError::TypeMismatch
        );
    }
}
