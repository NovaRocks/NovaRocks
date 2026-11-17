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
use crate::{
    BindingRequirements, CompileProfile, ControlShape, ImmutableExpressions, KernelAbiVersion,
    LocalProgramGraph, ProgramControlFlow, ProgramEvaluationRegion, ProgramExpressionRootSite,
    ProgramExpressionUse, ProgramNode, ProgramNodeExpressionRole, ProgramNodeId, ProgramNodeKind,
    ProgramRootControlBindings, ProgramRootUseBinding, StaticExprNode, StaticLayout, StaticValues,
};
use arrow_array::{RecordBatch, new_empty_array};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use novarocks_type_contract::{EvaluationRegionId, ExpressionEffectContext, ExpressionUseId};
use novarocks_types::SlotId;
use std::{collections::HashMap, num::NonZeroUsize, sync::Mutex};

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        Ok(())
    }
}
fn boolean(nullable: bool) -> FunctionValueType {
    FunctionValueType::new(DataType::Boolean, nullable)
}
fn integer(nullable: bool) -> FunctionValueType {
    FunctionValueType::new(DataType::Int64, nullable)
}
fn slot(ordinal: usize, ty: &FunctionValueType) -> StaticExprNode {
    StaticExprNode::new_value_typed(
        StaticExprKind::SlotId(SlotId::new(ordinal as u32 + 1)),
        ty.clone(),
    )
}
fn fixture(
    ne: bool,
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
        StaticExprNode::new_value_typed(
            if ne {
                StaticExprKind::Ne(ProgramExprId::new(0), ProgramExprId::new(1))
            } else {
                StaticExprKind::Eq(ProgramExprId::new(0), ProgramExprId::new(1))
            },
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
    assembled(nodes, types, root, root_type, demand)
}
// These are genuine root/control/resolved/type factories. Source slot facts
// remain explicit here; this does not assert lexical or runtime capability.
fn assembled(
    nodes: Vec<StaticExprNode>,
    types: Vec<FunctionArgumentType>,
    root: ProgramExprId,
    root_type: FunctionValueType,
    demand: EvaluationDemand,
) -> (
    ProgramResolvedCalls,
    BTreeMap<ProgramExpressionArena, Vec<FunctionArgumentType>>,
) {
    let arena = Arc::new(crate::test_support::arena(nodes).unwrap());
    let schema = Arc::new(Schema::new(vec![Field::new(
        "unused_input",
        DataType::Boolean,
        false,
    )]));
    let input = StaticLayout::try_new(schema.clone(), Arc::from([SlotId::new(1)])).unwrap();
    let values = StaticValues::try_new(
        RecordBatch::try_new(schema, vec![new_empty_array(&DataType::Boolean)]).unwrap(),
        input.clone(),
    )
    .unwrap();
    let output = StaticLayout::try_new(
        Arc::new(Schema::new(vec![root_type.try_to_field("result").unwrap()])),
        Arc::from([SlotId::new(2)]),
    )
    .unwrap();
    let (kind, layout, role) = match demand {
        EvaluationDemand::Value => (
            ProgramNodeKind::Project {
                input: ProgramNodeId::new(0),
                is_subordinate: false,
                validate_final_result_input: false,
                exprs: vec![root],
                expr_slot_ids: vec![SlotId::new(2)],
                expr_slot_schemas: None,
                output_indices: None,
            },
            output,
            ProgramNodeExpressionRole::ProjectOutput { expression: 0 },
        ),
        EvaluationDemand::TruthOnly => (
            ProgramNodeKind::Filter {
                input: ProgramNodeId::new(0),
                predicates: vec![root].into_boxed_slice(),
            },
            input.clone(),
            ProgramNodeExpressionRole::FilterPredicate { predicate: 0 },
        ),
    };
    let graph = LocalProgramGraph::try_new(
        vec![
            ProgramNode::new(0, ProgramNodeKind::Values { values }, input),
            ProgramNode::new(1, kind, layout.clone()),
        ],
        ProgramNodeId::new(1),
        arena.clone(),
        CompileProfile::new(
            NonZeroUsize::new(1).unwrap(),
            None,
            layout.identity().unwrap(),
            KernelAbiVersion::CURRENT,
        ),
        BindingRequirements::try_new(vec![]).unwrap(),
    )
    .unwrap();
    let mut uses = vec![];
    fn visit(
        arena: &ImmutableExpressions,
        definition: ProgramExprId,
        demand: EvaluationDemand,
        uses: &mut Vec<ProgramExpressionUse>,
    ) -> ExpressionUseId {
        let id = ExpressionUseId::new(uses.len() as u32);
        let (control, children) = match arena.node(definition).unwrap().kind() {
            StaticExprKind::Eq(left, right)
            | StaticExprKind::PreparedNullSafeComparison { left, right }
            | StaticExprKind::Ne(left, right)
            | StaticExprKind::Lt(left, right)
            | StaticExprKind::Le(left, right)
            | StaticExprKind::Gt(left, right)
            | StaticExprKind::Ge(left, right) => (ControlShape::Eager, vec![*left, *right]),
            StaticExprKind::PreparedCast { child, .. } => (ControlShape::Eager, vec![*child]),

            _ => (ControlShape::Eager, vec![]),
        };
        uses.push(ProgramExpressionUse {
            context: ExpressionEffectContext {
                use_id: id,
                region: EvaluationRegionId::new(0),
                demand,
            },
            definition,
            control,
            arguments: Box::default(),
        });
        let arguments = children
            .iter()
            .map(|child| visit(arena, *child, EvaluationDemand::Value, uses))
            .collect::<Vec<_>>();
        uses[id.get() as usize].arguments = arguments.into_boxed_slice();
        id
    }
    let root_use = visit(&arena, root, demand, &mut uses);
    let flow = ProgramControlFlow::try_new(
        vec![ProgramEvaluationRegion {
            id: EvaluationRegionId::new(0),
            parent: None,
            guard: None,
        }],
        uses,
        arena.nodes().len(),
        &Control,
    )
    .unwrap();
    let snapshot = ProgramRootControlBindings::try_new(
        graph,
        BTreeMap::from([(ProgramExpressionArena::Main, flow)]),
        vec![ProgramRootUseBinding {
            site: ProgramExpressionRootSite::Node {
                node: ProgramNodeId::new(1),
                role,
            },
            use_id: root_use,
        }],
        &Control,
    )
    .unwrap();
    (
        ProgramResolvedCalls::try_new(snapshot, vec![], &Control).unwrap(),
        BTreeMap::from([(ProgramExpressionArena::Main, types)]),
    )
}
fn checked(
    ne: bool,
    dead: bool,
    left: FunctionValueType,
    right: FunctionValueType,
    result: FunctionValueType,
    demand: EvaluationDemand,
) -> Result<ProgramTypedExpressions, ProgramExpressionTypeError> {
    let (calls, types) = fixture(ne, dead, left, right, result, demand);
    ProgramTypedExpressions::try_new(calls, types, &Control)
}

#[test]
fn eq_ne_value_and_truth_only_keep_exact_value_children_and_root_nullable_widening() {
    for ne in [false, true] {
        for demand in [EvaluationDemand::Value, EvaluationDemand::TruthOnly] {
            for (left_nullable, right_nullable) in
                [(false, false), (false, true), (true, false), (true, true)]
            {
                let typed = checked(
                    ne,
                    false,
                    integer(left_nullable),
                    integer(right_nullable),
                    boolean(left_nullable || right_nullable),
                    demand,
                )
                .unwrap();
                let flow =
                    &typed.resolved_calls().snapshot().flows()[&ProgramExpressionArena::Main];
                let root = &flow.uses()[&ExpressionUseId::new(0)];
                assert_eq!(root.context.demand, demand);
                assert_eq!(root.control, ControlShape::Eager);
                assert_eq!(root.arguments.len(), 2);
                for (ordinal, child) in root.arguments.iter().enumerate() {
                    assert_eq!(flow.uses()[child].definition, ProgramExprId::new(ordinal));
                    assert_eq!(flow.uses()[child].context.demand, EvaluationDemand::Value);
                }
                assert_eq!(
                    typed.definition_type(ProgramExpressionArena::Main, ProgramExprId::new(2)),
                    Some(&FunctionArgumentType::Value(boolean(
                        left_nullable || right_nullable
                    )))
                );
            }
            assert!(
                checked(
                    ne,
                    false,
                    integer(false),
                    integer(false),
                    boolean(true),
                    demand
                )
                .is_ok()
            );
        }
    }
}

#[test]
fn eq_ne_success_null_requires_nullable_bool_even_for_nonnullable_null_carriers() {
    for ne in [false, true] {
        for dead in [false, true] {
            for (left, right) in [
                (integer(true), integer(false)),
                (integer(false), integer(true)),
                (
                    FunctionValueType::new(DataType::Null, false),
                    FunctionValueType::new(DataType::Null, false),
                ),
            ] {
                assert!(matches!(
                    checked(
                        ne,
                        dead,
                        left.clone(),
                        right.clone(),
                        boolean(false),
                        EvaluationDemand::Value
                    ),
                    Err(ProgramExpressionTypeError::TypeMismatch)
                ));
                assert!(
                    checked(
                        ne,
                        dead,
                        left,
                        right,
                        boolean(true),
                        EvaluationDemand::Value
                    )
                    .is_ok()
                );
            }
        }
    }
}

#[test]
fn eq_ne_all_definitions_reject_nonboolean_output_root_domain_and_carrier_drift() {
    let uuid = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::Uuid,
    )
    .unwrap();
    let physical = FunctionValueType::new(DataType::FixedSizeBinary(16), false);
    let json =
        FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
            .unwrap();
    let pairs = [
        (uuid, physical),
        (json.clone(), FunctionValueType::new(DataType::Utf8, false)),
        (
            integer(false),
            FunctionValueType::new(DataType::Int32, false),
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
        (
            FunctionValueType::new(DataType::Decimal128(18, 2), false),
            FunctionValueType::new(DataType::Decimal128(18, 3), false),
        ),
    ];
    for ne in [false, true] {
        for dead in [false, true] {
            for (left, right) in &pairs {
                assert!(matches!(
                    checked(
                        ne,
                        dead,
                        left.clone(),
                        right.clone(),
                        boolean(true),
                        EvaluationDemand::Value
                    ),
                    Err(ProgramExpressionTypeError::TypeMismatch)
                ));
            }
            for result in [integer(false), json.clone()] {
                assert!(matches!(
                    checked(
                        ne,
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
}

#[allow(deprecated)]
fn nested(id: i64, annotation: &str, nullable: bool, logical: bool) -> FunctionValueType {
    let source = DataType::Struct(
        vec![
            Field::new_dict(
                "dictionary",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                nullable,
                id,
                true,
            )
            .with_metadata(HashMap::from([(
                "provider.annotation".into(),
                annotation.into(),
            )])),
            Field::new("payload", DataType::Utf8, true),
        ]
        .into(),
    );
    crate::test_value_types::string_structure_from_source(
        &[("dictionary", nullable, false), ("payload", true, logical)],
        false,
        &source,
    )
}
#[test]
fn eq_ne_preserve_complete_nested_identity_without_importing_runtime_flat_capability_gate() {
    let exact = nested(-91, "preserved", true, true);
    for ne in [false, true] {
        for dead in [false, true] {
            // Static type admissibility is independent of whether the current
            // prepared equality evaluator supports this nested carrier yet.
            assert!(
                checked(
                    ne,
                    dead,
                    exact.clone(),
                    exact.clone(),
                    boolean(true),
                    EvaluationDemand::Value
                )
                .is_ok()
            );
            for changed in [
                nested(-91, "preserved", false, true),
                nested(-91, "preserved", true, false),
            ] {
                assert!(matches!(
                    checked(
                        ne,
                        dead,
                        exact.clone(),
                        changed,
                        boolean(true),
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
                    checked(
                        ne,
                        dead,
                        exact.clone(),
                        encoding_only,
                        boolean(true),
                        EvaluationDemand::Value
                    )
                    .is_ok()
                );
            }
        }
    }
}

#[test]
fn eq_ne_callable_inputs_reject_at_actual_scalar_type_author_even_when_definition_has_no_use() {
    for ne in [false, true] {
        for dead in [false, true] {
            let ty = integer(false);
            let mut nodes = vec![
                slot(0, &ty),
                StaticExprNode::new_value_typed(
                    StaticExprKind::SlotId(SlotId::new(1)),
                    novarocks_functions::FunctionValueType::new(DataType::Int64, false),
                ),
                slot(2, &ty),
                StaticExprNode::new_value_typed(
                    if ne {
                        StaticExprKind::Ne(ProgramExprId::new(1), ProgramExprId::new(2))
                    } else {
                        StaticExprKind::Eq(ProgramExprId::new(1), ProgramExprId::new(2))
                    },
                    novarocks_functions::FunctionValueType::new(DataType::Boolean, false),
                ),
            ];
            let mut types = vec![
                FunctionArgumentType::Value(ty.clone()),
                FunctionArgumentType::Lambda {
                    parameter_types:
                        novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
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
            let (calls, types) =
                assembled(nodes, types, root, boolean(false), EvaluationDemand::Value);
            assert!(matches!(
                ProgramTypedExpressions::try_new(calls, types, &Control),
                Err(ProgramExpressionTypeError::WrongKind)
            ));
        }
    }
}

struct Trace {
    at: Option<usize>,
    cause: CompileControlError,
    seen: Mutex<Vec<(CompilePhase, u32)>>,
    refused: Mutex<bool>,
}
impl PureCompileControl for Trace {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut refused = self.refused.lock().unwrap();
        assert!(!*refused, "no callback after a primary refusal");
        let mut seen = self.seen.lock().unwrap();
        seen.push((phase, units));
        if self.at == Some(seen.len() - 1) {
            *refused = true;
            Err(self.cause.clone())
        } else {
            Ok(())
        }
    }
}
#[test]
fn eq_ne_actual_wide_type_walk_preserves_three_control_causes_at_every_callback_without_retry() {
    let wide = FunctionValueType::new(
        DataType::Struct(
            (0..320)
                .map(|i| Field::new(format!("actual_{i}"), DataType::Int64, false))
                .collect::<Vec<_>>()
                .into(),
        ),
        false,
    );
    for ne in [false, true] {
        let (calls, types) = fixture(
            ne,
            false,
            wide.clone(),
            wide.clone(),
            boolean(false),
            EvaluationDemand::Value,
        );
        let success = Trace {
            at: None,
            cause: CompileControlError::Cancelled,
            seen: Mutex::new(vec![]),
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

#[test]
fn invalid_eq_ordinary_error_observes_completed_tail_and_preserves_primary_control_refusal() {
    let (calls, types) = fixture(
        false,
        false,
        integer(false),
        integer(false),
        integer(false),
        EvaluationDemand::Value,
    );
    let recorder = Trace {
        at: None,
        cause: CompileControlError::Cancelled,
        seen: Mutex::new(vec![]),
        refused: Mutex::new(false),
    };
    assert!(matches!(
        ProgramTypedExpressions::try_new(calls.clone(), types.clone(), &recorder),
        Err(ProgramExpressionTypeError::TypeMismatch)
    ));
    let expected = recorder.seen.into_inner().unwrap();
    assert!(expected.len() > 1);
    // The small invalid definition never reaches a 256-unit quantum. Its
    // completed progress is published by the ordinary-error finish callback.
    assert!(expected.iter().all(|(_, units)| *units < 256));
    assert!(expected.last().is_some_and(|(_, units)| *units > 0));
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let control = Trace {
            at: Some(expected.len() - 1),
            cause: cause.clone(),
            seen: Mutex::new(vec![]),
            refused: Mutex::new(false),
        };
        assert!(matches!(
            ProgramTypedExpressions::try_new(calls.clone(), types.clone(), &control),
            Err(ProgramExpressionTypeError::Control(actual)) if actual == cause
        ));
        assert_eq!(*control.seen.lock().unwrap(), expected);
    }
}

#[path = "ordered_comparison_tests.rs"]
mod ordered_comparison_tests;

#[test]
fn dead_arithmetic_definitions_require_shared_full_types_and_nullable_result() {
    use novarocks_type_contract::{ArithmeticOperator as A, DecimalOverflowPolicy as P};
    let left = FunctionValueType::new(DataType::Int8, false);
    let right = FunctionValueType::new(DataType::Int16, false);
    for op in [A::Add, A::Subtract, A::Multiply, A::Divide, A::Modulo] {
        let expected =
            novarocks_type_contract::arithmetic_result_value_type_with_op(&left, &right, op)
                .unwrap();
        let expected = expected.with_nullable(true).unwrap();
        for invalid in 0..4 {
            let mut l = left.clone();
            let mut result = expected.clone();
            match invalid {
                0 => {}
                1 => result = result.with_nullable(false).unwrap(),
                2 => result = FunctionValueType::new(DataType::Int64, result.nullable()),
                3 => l = FunctionValueType::new(DataType::FixedSizeBinary(16), false),
                _ => unreachable!(),
            }
            let (calls, types) = assembled(
                vec![
                    slot(0, &l),
                    slot(1, &right),
                    StaticExprNode::new_value_typed(
                        StaticExprKind::PreparedArithmetic {
                            operator: op,
                            left: ProgramExprId::new(0),
                            right: ProgramExprId::new(1),
                            decimal_overflow_policy: P::ReportError,
                            allow_throw_exception: true,
                        },
                        result.clone(),
                    ),
                    slot(3, &boolean(false)),
                ],
                vec![
                    FunctionArgumentType::Value(l),
                    FunctionArgumentType::Value(right.clone()),
                    FunctionArgumentType::Value(result),
                    FunctionArgumentType::Value(boolean(false)),
                ],
                ProgramExprId::new(3),
                boolean(false),
                EvaluationDemand::Value,
            );
            let typed = ProgramTypedExpressions::try_new(calls, types, &Control);
            if invalid == 0 {
                assert!(typed.is_ok());
            } else {
                assert_eq!(typed.unwrap_err(), ProgramExpressionTypeError::TypeMismatch);
            }
        }
    }
}

#[test]
fn dead_cast_definitions_preserve_exact_identity_and_successful_narrowing_null() {
    use novarocks_functions::CastOperation;
    use novarocks_type_contract::DecimalOverflowPolicy;
    let labelled = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    for (source, result, valid) in [
        (
            FunctionValueType::new(DataType::Int8, false),
            FunctionValueType::new(DataType::Int64, false),
            true,
        ),
        (
            FunctionValueType::new(DataType::Int64, false),
            FunctionValueType::new(DataType::Int8, true),
            true,
        ),
        (
            FunctionValueType::new(DataType::Int64, false),
            FunctionValueType::new(DataType::Int8, false),
            false,
        ),
        (
            FunctionValueType::new(DataType::Int8, true),
            FunctionValueType::new(DataType::Float32, false),
            false,
        ),
        (
            FunctionValueType::new(DataType::Null, false),
            FunctionValueType::new(DataType::Int64, false),
            false,
        ),
        (
            FunctionValueType::new(DataType::Null, false),
            FunctionValueType::new(DataType::Int64, true),
            true,
        ),
        (
            labelled.clone(),
            FunctionValueType::new(DataType::FixedSizeBinary(16), true),
            false,
        ),
        (labelled.clone(), labelled.clone(), true),
        // A dead static type rule does not become the installed 24-pair runtime subset.
        (
            FunctionValueType::new(DataType::Utf8, true),
            FunctionValueType::new(DataType::Int64, true),
            true,
        ),
    ] {
        let (calls, types) = assembled(
            vec![
                slot(0, &source),
                StaticExprNode::new_value_typed(
                    StaticExprKind::PreparedCast {
                        operation: CastOperation::Carrier,
                        child: ProgramExprId::new(0),
                        decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                        allow_throw_exception: true,
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
            ProgramExprId::new(2),
            boolean(false),
            EvaluationDemand::Value,
        );
        let typed = ProgramTypedExpressions::try_new(calls, types, &Control);
        if valid {
            assert!(typed.is_ok(), "{typed:?}");
        } else {
            assert_eq!(typed.unwrap_err(), ProgramExpressionTypeError::TypeMismatch);
        }
    }
}

#[path = "nullsafe_tests.rs"]
mod nullsafe_tests;

#[path = "cast_bool_tests.rs"]
mod cast_bool_tests;

#[test]
fn float_integer_cast_all_definitions_use_the_frozen_allow_mode_for_successful_null() {
    use novarocks_functions::CastOperation;
    use novarocks_type_contract::DecimalOverflowPolicy as P;
    for source_type in [DataType::Float32, DataType::Float64] {
        for target_type in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
        ] {
            for allow in [false, true] {
                for source_nullable in [false, true] {
                    for result_nullable in [false, true] {
                        for policy in [P::OutputNull, P::ReportError] {
                            for dead in [false, true] {
                                let source =
                                    FunctionValueType::new(source_type.clone(), source_nullable);
                                let result =
                                    FunctionValueType::new(target_type.clone(), result_nullable);
                                let root = if dead {
                                    ProgramExprId::new(2)
                                } else {
                                    ProgramExprId::new(1)
                                };
                                let root_type = if dead { boolean(false) } else { result.clone() };
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
                                        FunctionArgumentType::Value(source),
                                        FunctionArgumentType::Value(result),
                                        FunctionArgumentType::Value(boolean(false)),
                                    ],
                                    root,
                                    root_type,
                                    EvaluationDemand::Value,
                                );
                                let typed =
                                    ProgramTypedExpressions::try_new(calls, types, &Control);
                                let valid = result_nullable || (!source_nullable && allow);
                                if valid {
                                    assert!(
                                        typed.is_ok(),
                                        "{source_type:?}->{target_type:?} allow={allow}, dead={dead}: {typed:?}"
                                    );
                                } else {
                                    assert_eq!(
                                        typed.unwrap_err(),
                                        ProgramExpressionTypeError::TypeMismatch
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

#[path = "cast_unsigned_tests.rs"]
mod cast_unsigned_tests;

#[path = "cast_timestamp_tests.rs"]
mod cast_timestamp_tests;
