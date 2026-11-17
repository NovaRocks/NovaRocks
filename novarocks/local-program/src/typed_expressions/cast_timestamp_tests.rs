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

#[test]
fn timestamp_cast_all_sixteen_live_and_dead_definitions_keep_successful_null_contracts() {
    let units = [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ];
    // Original successful-NULL outcomes, rather than runtime installation.
    // Microsecond -> nanosecond has a row error instead of successful NULL.
    let successful_null = [
        [false, true, true, true],
        [false, false, true, true],
        [false, false, false, false],
        [false, false, false, false],
    ];
    let mut definitions = 0;
    for (source_index, source_unit) in units.iter().enumerate() {
        for (target_index, target_unit) in units.iter().enumerate() {
            for source_nullable in [false, true] {
                for result_nullable in [false, true] {
                    for allow in [false, true] {
                        for policy in [
                            DecimalOverflowPolicy::OutputNull,
                            DecimalOverflowPolicy::ReportError,
                        ] {
                            for dead in [false, true] {
                                definitions += 1;
                                let source = FunctionValueType::new(
                                    DataType::Timestamp(*source_unit, None),
                                    source_nullable,
                                );
                                let result = FunctionValueType::new(
                                    DataType::Timestamp(*target_unit, None),
                                    result_nullable,
                                );
                                let (calls, types) = assembled(
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
                                        FunctionArgumentType::Value(source.clone()),
                                        FunctionArgumentType::Value(result.clone()),
                                        FunctionArgumentType::Value(boolean(false)),
                                    ],
                                    ProgramExprId::new(if dead { 2 } else { 1 }),
                                    if dead { boolean(false) } else { result.clone() },
                                    EvaluationDemand::Value,
                                );
                                let typed =
                                    ProgramTypedExpressions::try_new(calls, types, &Control);
                                let valid = result_nullable
                                    || (!source_nullable
                                        && !successful_null[source_index][target_index]);
                                if !valid {
                                    assert_eq!(
                                        typed.unwrap_err(),
                                        ProgramExpressionTypeError::TypeMismatch,
                                        "{source_unit:?}->{target_unit:?} source_nullable={source_nullable} result_nullable={result_nullable} allow={allow} policy={policy:?} dead={dead}"
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
                                    matches!(snapshot.roots().arenas()[&arena].node(ProgramExprId::new(1)).unwrap().kind(),
                                    StaticExprKind::PreparedCast { operation: CastOperation::Carrier, child, decimal_overflow_policy, allow_throw_exception }
                                    if *child == ProgramExprId::new(0) && *decimal_overflow_policy == policy && *allow_throw_exception == allow)
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
        }
    }
    assert_eq!(definitions, 512);
}
