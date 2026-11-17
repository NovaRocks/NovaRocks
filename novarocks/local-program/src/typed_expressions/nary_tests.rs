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
use arrow_array::{BooleanArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use novarocks_functions::{ConstantPolicy, ConstantValue};
use novarocks_type_contract::{EvaluationRegionId, ExpressionEffectContext, ExpressionUseId};
use novarocks_types::SlotId;
use std::{collections::HashMap, num::NonZeroUsize};

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, work: u32) -> Result<(), CompileControlError> {
        assert!(work <= 256);
        Ok(())
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 1,
        max_array_nodes: 32,
        max_logical_elements: 32,
        max_retained_buffer_bytes: 8192,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 64,
        max_metadata_bytes: 8192,
        max_library_validation_work: 8192,
        max_library_validation_bytes: 8192,
    }
}
fn boolean(value: Option<bool>, nullable: bool) -> (StaticExprNode, FunctionArgumentType) {
    let ty = FunctionValueType::new(DataType::Boolean, nullable);
    let field = Arc::new(
        Field::new("actual_boolean", DataType::Boolean, nullable).with_metadata(HashMap::from([(
            "fixture_annotation".to_owned(),
            "preserved".to_owned(),
        )])),
    );
    let constant = match value {
        Some(value) => ConstantValue::from_boolean(
            field,
            ty.clone(),
            value,
            policy(),
            CompilePhase::LowerProgram,
            &Control,
        ),
        None => ConstantValue::null(
            field,
            ty.clone(),
            policy(),
            CompilePhase::LowerProgram,
            &Control,
        ),
    }
    .unwrap();
    (
        StaticExprNode::new(StaticExprKind::Constant(constant), DataType::Boolean, None),
        FunctionArgumentType::Value(ty),
    )
}
fn non_boolean(json: bool) -> (StaticExprNode, FunctionArgumentType) {
    let ty = if json {
        FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
            .unwrap()
    } else {
        FunctionValueType::new(DataType::Int64, false)
    };
    let field = Arc::new(ty.try_to_field("actual_non_boolean").unwrap());
    let constant = if json {
        ConstantValue::from_utf8(
            field,
            ty.clone(),
            "{}",
            policy(),
            CompilePhase::LowerProgram,
            &Control,
        )
    } else {
        ConstantValue::from_i64(
            field,
            ty.clone(),
            1,
            policy(),
            CompilePhase::LowerProgram,
            &Control,
        )
    }
    .unwrap();
    (
        StaticExprNode::new(
            StaticExprKind::Constant(constant),
            ty.data_type().clone(),
            None,
        ),
        FunctionArgumentType::Value(ty),
    )
}

fn fixture(
    or: bool,
    leaves: Vec<(StaticExprNode, FunctionArgumentType)>,
    root_type: FunctionValueType,
    demand: EvaluationDemand,
) -> (
    ProgramResolvedCalls,
    BTreeMap<ProgramExpressionArena, Vec<FunctionArgumentType>>,
) {
    let (mut nodes, mut types): (Vec<_>, Vec<_>) = leaves.into_iter().unzip();
    let root = ProgramExprId::new(nodes.len());
    let args = (0..nodes.len()).map(ProgramExprId::new).collect();
    nodes.push(StaticExprNode::new(
        if or {
            StaticExprKind::NaryOr { args }
        } else {
            StaticExprKind::NaryAnd { args }
        },
        root_type.data_type().clone(),
        None,
    ));
    types.push(FunctionArgumentType::Value(root_type.clone()));
    assembled(nodes, types, root, root_type, demand)
}
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
    let arena =
        Arc::new(ImmutableExpressions::try_new(nodes, false, HashMap::new(), None).unwrap());
    let source_schema = Arc::new(Schema::new(vec![Field::new(
        "unused_input",
        DataType::Boolean,
        false,
    )]));
    let source_layout =
        StaticLayout::try_new(source_schema.clone(), Arc::from([SlotId::new(1)])).unwrap();
    let values = StaticValues::try_new(
        RecordBatch::try_new(
            source_schema,
            vec![Arc::new(BooleanArray::from(vec![true]))],
        )
        .unwrap(),
        source_layout.clone(),
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
            output.clone(),
            ProgramNodeExpressionRole::ProjectOutput { expression: 0 },
        ),
        EvaluationDemand::TruthOnly => (
            ProgramNodeKind::Filter {
                input: ProgramNodeId::new(0),
                predicates: vec![root].into_boxed_slice(),
            },
            source_layout.clone(),
            ProgramNodeExpressionRole::FilterPredicate { predicate: 0 },
        ),
    };
    let program = LocalProgramGraph::try_new(
        vec![
            ProgramNode::new(0, ProgramNodeKind::Values { values }, source_layout),
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
    let mut uses = Vec::new();
    let root_use = add_use(root, demand, &arena, &mut uses);
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
        program,
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
fn add_use(
    definition: ProgramExprId,
    demand: EvaluationDemand,
    arena: &ImmutableExpressions,
    uses: &mut Vec<ProgramExpressionUse>,
) -> ExpressionUseId {
    let id = ExpressionUseId::new(uses.len() as u32);
    let (control, arguments): (_, Vec<_>) = match arena.node(definition).unwrap().kind() {
        StaticExprKind::NaryAnd { args } => (ControlShape::Conjunction, args.clone()),
        StaticExprKind::NaryOr { args } => (ControlShape::Disjunction, args.clone()),
        StaticExprKind::LambdaFunction { body, .. } => (ControlShape::LambdaBody, vec![*body]),
        StaticExprKind::Not(child)
        | StaticExprKind::IsNull(child)
        | StaticExprKind::IsNotNull(child) => (ControlShape::Eager, vec![*child]),
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
    let children = arguments
        .into_iter()
        .map(|child| {
            add_use(
                child,
                if control == ControlShape::Eager {
                    EvaluationDemand::Value
                } else {
                    demand
                },
                arena,
                uses,
            )
        })
        .collect();
    uses[id.get() as usize].arguments = children;
    id
}

#[path = "unary_tests.rs"]
mod unary_tests;

#[test]
fn nary_boolean_value_and_truth_only_preserve_full_nullable_types_and_constant_metadata() {
    for or in [false, true] {
        for demand in [EvaluationDemand::Value, EvaluationDemand::TruthOnly] {
            let (calls, types) = fixture(
                or,
                vec![
                    boolean(Some(true), false),
                    boolean(None, true),
                    boolean(Some(false), false),
                ],
                FunctionValueType::new(DataType::Boolean, true),
                demand,
            );
            let typed = ProgramTypedExpressions::try_new(calls, types, &Control).unwrap();
            assert_eq!(
                typed.definition_type(ProgramExpressionArena::Main, ProgramExprId::new(3)),
                Some(&FunctionArgumentType::Value(FunctionValueType::new(
                    DataType::Boolean,
                    true
                )))
            );
            let arena =
                &typed.resolved_calls().snapshot().roots().arenas()[&ProgramExpressionArena::Main];
            let StaticExprKind::Constant(value) = arena.node(ProgramExprId::new(0)).unwrap().kind()
            else {
                panic!("actual CV")
            };
            assert_eq!(value.field().metadata()["fixture_annotation"], "preserved");
            assert_eq!(
                typed.resolved_calls().snapshot().flows()[&ProgramExpressionArena::Main].uses()
                    [&ExpressionUseId::new(0)]
                    .context
                    .demand,
                demand
            );
        }
    }
}

#[test]
fn nary_nonnullable_result_requires_no_nullable_operand_or_actual_deciding_boolean_cv() {
    for or in [false, true] {
        for demand in [EvaluationDemand::Value, EvaluationDemand::TruthOnly] {
            for nullable in [false, true] {
                let (calls, types) = fixture(
                    or,
                    vec![boolean(None, true), boolean(Some(or), nullable)],
                    FunctionValueType::new(DataType::Boolean, false),
                    demand,
                );
                let typed = ProgramTypedExpressions::try_new(calls, types, &Control).unwrap();
                assert_eq!(
                    typed.definition_type(ProgramExpressionArena::Main, ProgramExprId::new(2)),
                    Some(&FunctionArgumentType::Value(FunctionValueType::new(
                        DataType::Boolean,
                        false
                    )))
                );
            }
            let (calls, types) = fixture(
                or,
                vec![boolean(Some(!or), false), boolean(Some(!or), false)],
                FunctionValueType::new(DataType::Boolean, false),
                demand,
            );
            ProgramTypedExpressions::try_new(calls, types, &Control).unwrap();
        }
    }
}

#[test]
fn truth_only_cannot_rebrand_nullable_result_using_null_or_nondeciding_cv() {
    for or in [false, true] {
        for demand in [EvaluationDemand::Value, EvaluationDemand::TruthOnly] {
            for other in [None, Some(!or)] {
                let (calls, types) = fixture(
                    or,
                    vec![boolean(None, true), boolean(other, true)],
                    FunctionValueType::new(DataType::Boolean, false),
                    demand,
                );
                assert_eq!(
                    ProgramTypedExpressions::try_new(calls, types, &Control).unwrap_err(),
                    ProgramExpressionTypeError::TypeMismatch
                );
            }
        }
    }
}

#[test]
fn every_nary_operand_and_result_requires_complete_physical_boolean_type() {
    for or in [false, true] {
        for demand in [EvaluationDemand::Value, EvaluationDemand::TruthOnly] {
            for json in [false, true] {
                let (calls, types) = fixture(
                    or,
                    vec![boolean(Some(or), false), non_boolean(json)],
                    FunctionValueType::new(DataType::Boolean, false),
                    demand,
                );
                assert_eq!(
                    ProgramTypedExpressions::try_new(calls, types, &Control).unwrap_err(),
                    ProgramExpressionTypeError::TypeMismatch
                );
                let root_type = if json {
                    FunctionValueType::try_with_logical_type(
                        DataType::Utf8,
                        true,
                        ValueLogicalType::Json,
                    )
                    .unwrap()
                } else {
                    FunctionValueType::new(DataType::Int64, true)
                };
                let (calls, types) = fixture(
                    or,
                    vec![boolean(Some(true), false), boolean(Some(false), false)],
                    root_type,
                    demand,
                );
                assert_eq!(
                    ProgramTypedExpressions::try_new(calls, types, &Control).unwrap_err(),
                    ProgramExpressionTypeError::TypeMismatch
                );
            }
        }
    }
}

#[test]
fn nary_boolean_rejects_lambda_operands_even_with_boolean_result_domain() {
    for or in [false, true] {
        for demand in [EvaluationDemand::Value, EvaluationDemand::TruthOnly] {
            let (body, body_type) = boolean(Some(true), false);
            let root_type = FunctionValueType::new(DataType::Boolean, false);
            let (calls, types) = assembled(
                vec![
                    body,
                    StaticExprNode::new(
                        StaticExprKind::LambdaFunction {
                            body: ProgramExprId::new(0),
                            arg_slots: vec![],
                            common_sub_exprs: vec![],
                            is_nondeterministic: false,
                        },
                        DataType::Boolean,
                        None,
                    ),
                    StaticExprNode::new(
                        if or {
                            StaticExprKind::NaryOr {
                                args: vec![ProgramExprId::new(1)],
                            }
                        } else {
                            StaticExprKind::NaryAnd {
                                args: vec![ProgramExprId::new(1)],
                            }
                        },
                        DataType::Boolean,
                        None,
                    ),
                ],
                vec![
                    body_type,
                    FunctionArgumentType::Lambda {
                        parameter_types:
                            novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
                                Box::default(),
                            ),
                        result_type: root_type.clone(),
                    },
                    FunctionArgumentType::Value(root_type.clone()),
                ],
                ProgramExprId::new(2),
                root_type,
                demand,
            );
            assert_eq!(
                ProgramTypedExpressions::try_new(calls, types, &Control).unwrap_err(),
                ProgramExpressionTypeError::WrongKind
            );
        }
    }
}
