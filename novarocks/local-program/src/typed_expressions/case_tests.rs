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
    ProgramRootControlBindings, ProgramRootUseBinding, RegionGuard, StaticExprNode, StaticLayout,
    StaticValues,
};
use arrow_array::{RecordBatch, new_empty_array};
use arrow_schema::{DataType, Field, Schema};
use novarocks_type_contract::{
    EvaluationRegionId, ExpressionEffectContext, ExpressionUseId, GuardKind,
    control_argument_semantics,
};
use novarocks_types::SlotId;
use std::{collections::HashMap, num::NonZeroUsize, sync::Mutex};

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, work: u32) -> Result<(), CompileControlError> {
        assert!(work <= 256);
        Ok(())
    }
}
fn boolean(nullable: bool) -> FunctionValueType {
    FunctionValueType::new(DataType::Boolean, nullable)
}
fn json(nullable: bool) -> FunctionValueType {
    FunctionValueType::try_with_logical_type(DataType::Utf8, nullable, ValueLogicalType::Json)
        .unwrap()
}
fn integer(nullable: bool) -> FunctionValueType {
    FunctionValueType::new(DataType::Int64, nullable)
}

// These fixtures enter the actual public root/control and mandatory type
// factories. Slot types are explicit source facts, not a lexical/runtime proof.
fn fixture(
    simple: bool,
    has_else: bool,
    children: Vec<FunctionValueType>,
    result: FunctionValueType,
    demand: EvaluationDemand,
) -> (
    ProgramResolvedCalls,
    BTreeMap<ProgramExpressionArena, Vec<FunctionArgumentType>>,
) {
    let mut nodes = Vec::new();
    let mut types = Vec::new();
    for (ordinal, value) in children.into_iter().enumerate() {
        nodes.push(StaticExprNode::new_value_typed(
            StaticExprKind::SlotId(SlotId::new(ordinal as u32 + 1)),
            value.clone(),
        ));
        types.push(FunctionArgumentType::Value(value));
    }
    let root = ProgramExprId::new(nodes.len());
    nodes.push(StaticExprNode::new_value_typed(
        StaticExprKind::Case {
            has_case_expr: simple,
            has_else_expr: has_else,
            children: (0..nodes.len()).map(ProgramExprId::new).collect(),
        },
        result.clone(),
    ));
    types.push(FunctionArgumentType::Value(result.clone()));
    assembled(nodes, types, root, result, demand)
}
fn assembled(
    nodes: Vec<StaticExprNode>,
    types: Vec<FunctionArgumentType>,
    root: ProgramExprId,
    result: FunctionValueType,
    demand: EvaluationDemand,
) -> (
    ProgramResolvedCalls,
    BTreeMap<ProgramExpressionArena, Vec<FunctionArgumentType>>,
) {
    let arena = Arc::new(crate::test_support::arena(nodes).unwrap());
    let input_schema = Arc::new(Schema::new(vec![Field::new(
        "input",
        DataType::Boolean,
        false,
    )]));
    let input_layout =
        StaticLayout::try_new(input_schema.clone(), Arc::from([SlotId::new(1)])).unwrap();
    let values = StaticValues::try_new(
        RecordBatch::try_new(input_schema, vec![new_empty_array(&DataType::Boolean)]).unwrap(),
        input_layout.clone(),
    )
    .unwrap();
    let result_layout = StaticLayout::try_new(
        Arc::new(Schema::new(vec![result.try_to_field("result").unwrap()])),
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
            result_layout,
            ProgramNodeExpressionRole::ProjectOutput { expression: 0 },
        ),
        EvaluationDemand::TruthOnly => (
            ProgramNodeKind::Filter {
                input: ProgramNodeId::new(0),
                predicates: vec![root].into_boxed_slice(),
            },
            input_layout.clone(),
            ProgramNodeExpressionRole::FilterPredicate { predicate: 0 },
        ),
    };
    let graph = LocalProgramGraph::try_new(
        vec![
            ProgramNode::new(0, ProgramNodeKind::Values { values }, input_layout),
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
    let mut domains = vec![ProgramEvaluationRegion {
        id: EvaluationRegionId::new(0),
        parent: None,
        guard: None,
    }];
    let root_use = add_use(
        root,
        demand,
        EvaluationRegionId::new(0),
        &arena,
        &mut uses,
        &mut domains,
    );
    let flow = ProgramControlFlow::try_new(domains, uses, arena.nodes().len(), &Control).unwrap();
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
fn add_use(
    definition: ProgramExprId,
    demand: EvaluationDemand,
    domain: EvaluationRegionId,
    arena: &ImmutableExpressions,
    uses: &mut Vec<ProgramExpressionUse>,
    domains: &mut Vec<ProgramEvaluationRegion>,
) -> ExpressionUseId {
    let id = ExpressionUseId::new(uses.len() as u32);
    let (shape, children) = match arena.node(definition).unwrap().kind() {
        StaticExprKind::Case {
            has_case_expr,
            has_else_expr,
            children,
        } => (
            ControlShape::Case {
                simple: *has_case_expr,
                arms: ((children.len() - usize::from(*has_case_expr) - usize::from(*has_else_expr))
                    / 2) as u32,
                has_else: *has_else_expr,
            },
            children.clone(),
        ),

        _ => (ControlShape::Eager, vec![]),
    };
    uses.push(ProgramExpressionUse {
        context: ExpressionEffectContext {
            use_id: id,
            region: domain,
            demand,
        },
        definition,
        control: shape,
        arguments: Box::default(),
    });
    let mut arguments = Vec::new();
    for (ordinal, child) in children.iter().enumerate() {
        let (child_demand, guard) =
            control_argument_semantics(shape, children.len(), ordinal, demand).unwrap();
        let child_region = if let Some(kind) = guard {
            let child_region = EvaluationRegionId::new(domains.len() as u32);
            domains.push(ProgramEvaluationRegion {
                id: child_region,
                parent: Some(domain),
                guard: Some(RegionGuard { owner: id, kind }),
            });
            child_region
        } else {
            domain
        };
        arguments.push(add_use(
            *child,
            child_demand,
            child_region,
            arena,
            uses,
            domains,
        ));
    }
    uses[id.get() as usize].arguments = arguments.into_boxed_slice();
    id
}
fn checked(
    simple: bool,
    has_else: bool,
    children: Vec<FunctionValueType>,
    result: FunctionValueType,
    demand: EvaluationDemand,
) -> Result<ProgramTypedExpressions, ProgramExpressionTypeError> {
    let (calls, types) = fixture(simple, has_else, children, result, demand);
    ProgramTypedExpressions::try_new(calls, types, &Control)
}

#[test]
fn searched_case_value_and_truth_only_keep_actual_guarded_demands_and_nullable_result() {
    for demand in [EvaluationDemand::Value, EvaluationDemand::TruthOnly] {
        let typed = checked(
            false,
            true,
            vec![
                boolean(true),
                boolean(false),
                boolean(false),
                boolean(true),
                boolean(false),
            ],
            boolean(true),
            demand,
        )
        .unwrap();
        let flow = &typed.resolved_calls().snapshot().flows()[&ProgramExpressionArena::Main];
        let root = &flow.uses()[&ExpressionUseId::new(0)];
        assert_eq!(
            root.control,
            ControlShape::Case {
                simple: false,
                arms: 2,
                has_else: true
            }
        );
        for (ordinal, child) in root.arguments.iter().enumerate() {
            let child = &flow.uses()[child];
            assert_eq!(
                child.context.demand,
                if ordinal < 4 && ordinal % 2 == 0 {
                    EvaluationDemand::TruthOnly
                } else {
                    demand
                }
            );
            let kind = match ordinal {
                0 => GuardKind::CaseWhen { arm: 0 },
                1 => GuardKind::CaseThen { arm: 0 },
                2 => GuardKind::CaseWhen { arm: 1 },
                3 => GuardKind::CaseThen { arm: 1 },
                _ => GuardKind::CaseElse,
            };
            assert_eq!(
                flow.regions()[&child.context.region].guard,
                Some(RegionGuard {
                    owner: root.context.use_id,
                    kind
                })
            );
        }
        assert_eq!(
            typed.definition_type(ProgramExpressionArena::Main, ProgramExprId::new(5)),
            Some(&FunctionArgumentType::Value(boolean(true)))
        );
    }
}

#[test]
fn simple_case_labels_ignore_only_outer_nullable_and_keep_actual_value_demand() {
    for demand in [EvaluationDemand::Value, EvaluationDemand::TruthOnly] {
        let typed = checked(
            true,
            true,
            vec![
                json(false),
                json(true),
                boolean(false),
                json(false),
                boolean(true),
                boolean(false),
            ],
            boolean(true),
            demand,
        )
        .unwrap();
        let flow = &typed.resolved_calls().snapshot().flows()[&ProgramExpressionArena::Main];
        let root = &flow.uses()[&ExpressionUseId::new(0)];
        for ordinal in [0, 1, 3] {
            assert_eq!(
                flow.uses()[&root.arguments[ordinal]].context.demand,
                EvaluationDemand::Value
            );
        }
        assert_eq!(
            flow.uses()[&root.arguments[0]].context.region,
            root.context.region
        );
    }
    assert!(
        checked(
            true,
            true,
            vec![json(true), json(false), json(false), json(true)],
            json(true),
            EvaluationDemand::Value
        )
        .is_ok()
    );
    assert!(matches!(
        checked(
            true,
            true,
            vec![
                json(false),
                FunctionValueType::new(DataType::Utf8, true),
                boolean(false),
                boolean(false)
            ],
            boolean(false),
            EvaluationDemand::Value
        ),
        Err(ProgramExpressionTypeError::TypeMismatch)
    ));
    assert!(matches!(
        checked(
            true,
            true,
            vec![
                integer(false),
                FunctionValueType::new(DataType::Int32, false),
                boolean(false),
                boolean(false)
            ],
            boolean(false),
            EvaluationDemand::Value
        ),
        Err(ProgramExpressionTypeError::TypeMismatch)
    ));
}

#[allow(deprecated)]
fn nested(id: i64, annotation: &str, child_nullable: bool, child_json: bool) -> FunctionValueType {
    let source = DataType::Struct(
        vec![
            Field::new_dict(
                "dictionary",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                child_nullable,
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
        &[
            ("dictionary", child_nullable, false),
            ("payload", true, child_json),
        ],
        true,
        &source,
    )
}

#[test]
fn case_domains_compare_nested_metadata_dictionary_identity_nullability_and_logical_roots_exactly()
{
    let exact = nested(-91, "kept", true, true);
    let typed = checked(
        true,
        true,
        vec![exact.clone(), exact.clone(), exact.clone(), exact.clone()],
        exact.clone(),
        EvaluationDemand::Value,
    )
    .unwrap();
    assert_eq!(
        typed.definition_type(ProgramExpressionArena::Main, ProgramExprId::new(4)),
        Some(&FunctionArgumentType::Value(exact.clone()))
    );
    for changed in [
        nested(-91, "kept", false, true),
        nested(-91, "kept", true, false),
    ] {
        assert!(matches!(
            checked(
                true,
                true,
                vec![exact.clone(), changed.clone(), exact.clone(), exact.clone()],
                exact.clone(),
                EvaluationDemand::Value
            ),
            Err(ProgramExpressionTypeError::TypeMismatch)
        ));
        assert!(matches!(
            checked(
                false,
                true,
                vec![boolean(false), changed.clone(), exact.clone()],
                exact.clone(),
                EvaluationDemand::Value
            ),
            Err(ProgramExpressionTypeError::TypeMismatch)
        ));
        assert!(matches!(
            checked(
                false,
                true,
                vec![boolean(false), exact.clone(), changed],
                exact.clone(),
                EvaluationDemand::Value
            ),
            Err(ProgramExpressionTypeError::TypeMismatch)
        ));
    }
    for encoding_only in [
        nested(0, "kept", true, true),
        nested(-91, "changed", true, true),
    ] {
        assert!(
            checked(
                true,
                true,
                vec![exact.clone(), encoding_only, exact.clone(), exact.clone()],
                exact.clone(),
                EvaluationDemand::Value
            )
            .is_ok()
        );
    }
    assert!(matches!(
        checked(
            false,
            true,
            vec![
                boolean(false),
                json(false),
                FunctionValueType::new(DataType::Utf8, false)
            ],
            json(false),
            EvaluationDemand::Value
        ),
        Err(ProgramExpressionTypeError::TypeMismatch)
    ));
}

#[test]
fn case_result_nullability_cannot_narrow_any_arm_or_implicit_else() {
    for demand in [EvaluationDemand::Value, EvaluationDemand::TruthOnly] {
        assert!(
            checked(
                false,
                true,
                vec![boolean(true), boolean(false), boolean(false)],
                boolean(false),
                demand
            )
            .is_ok()
        );
        assert!(
            checked(
                false,
                true,
                vec![boolean(false), boolean(false), boolean(false)],
                boolean(true),
                demand
            )
            .is_ok()
        );
        for children in [
            vec![boolean(false), boolean(true), boolean(false)],
            vec![boolean(false), boolean(false), boolean(true)],
        ] {
            assert!(matches!(
                checked(false, true, children, boolean(false), demand),
                Err(ProgramExpressionTypeError::TypeMismatch)
            ));
        }
        assert!(
            checked(
                false,
                false,
                vec![boolean(false), boolean(false)],
                boolean(true),
                demand
            )
            .is_ok()
        );
        assert!(matches!(
            checked(
                false,
                false,
                vec![boolean(false), boolean(false)],
                boolean(false),
                demand
            ),
            Err(ProgramExpressionTypeError::TypeMismatch)
        ));
    }
    assert!(matches!(
        checked(
            false,
            true,
            vec![integer(false), boolean(false), boolean(false)],
            boolean(false),
            EvaluationDemand::Value
        ),
        Err(ProgramExpressionTypeError::TypeMismatch)
    ));
    // TruthOnly is a usage constraint, not permission to rebrand a JSON result.
    assert!(matches!(
        checked(
            false,
            true,
            vec![boolean(false), json(false), json(false)],
            json(false),
            EvaluationDemand::TruthOnly
        ),
        Err(ProgramExpressionTypeError::WrongDemand)
    ));
}

#[test]
fn actual_case_value_positions_reject_lambda_wrappers_in_operand_labels_and_results() {
    for simple in [false, true] {
        let count = if simple { 4 } else { 3 };
        for position in 0..count {
            // The Lambda has a real earlier Boolean body and its own actual
            // LambdaBody use. Its result carrier is not a scalar argument.
            let mut nodes = vec![StaticExprNode::new_value_typed(
                StaticExprKind::SlotId(SlotId::new(1)),
                novarocks_functions::FunctionValueType::new(DataType::Boolean, false),
            )];
            let mut types = vec![FunctionArgumentType::Value(boolean(false))];
            for ordinal in 0..count {
                if ordinal == position {
                    nodes.push(StaticExprNode::new_value_typed(
                        StaticExprKind::SlotId(SlotId::new(1)),
                        novarocks_functions::FunctionValueType::new(DataType::Boolean, false),
                    ));
                    types.push(FunctionArgumentType::Lambda {
                        parameter_types:
                            novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
                                Box::default(),
                            ),
                        result_type: boolean(false),
                    });
                } else {
                    nodes.push(StaticExprNode::new_value_typed(
                        StaticExprKind::SlotId(SlotId::new(1)),
                        novarocks_functions::FunctionValueType::new(DataType::Boolean, false),
                    ));
                    types.push(FunctionArgumentType::Value(boolean(false)));
                }
            }
            let root = ProgramExprId::new(nodes.len());
            nodes.push(StaticExprNode::new_value_typed(
                StaticExprKind::Case {
                    has_case_expr: simple,
                    has_else_expr: true,
                    children: (1..=count).map(ProgramExprId::new).collect(),
                },
                novarocks_functions::FunctionValueType::new(DataType::Boolean, false),
            ));
            types.push(FunctionArgumentType::Value(boolean(false)));
            let (calls, types) =
                assembled(nodes, types, root, boolean(false), EvaluationDemand::Value);
            assert!(matches!(
                ProgramTypedExpressions::try_new(calls, types, &Control),
                Err(ProgramExpressionTypeError::WrongKind)
            ));
        }
    }
}

#[test]
fn dead_case_definitions_cannot_bypass_flags_arity_or_value_type_validation() {
    for (simple, has_else, count) in [
        (false, false, 0),
        (false, false, 1),
        (false, true, 2),
        (true, false, 2),
        (true, true, 3),
    ] {
        let mut nodes = vec![StaticExprNode::new_value_typed(
            StaticExprKind::SlotId(SlotId::new(1)),
            novarocks_functions::FunctionValueType::new(DataType::Boolean, false),
        )];
        let mut types = vec![FunctionArgumentType::Value(boolean(false))];
        for _ in 0..count {
            nodes.push(StaticExprNode::new_value_typed(
                StaticExprKind::SlotId(SlotId::new(1)),
                novarocks_functions::FunctionValueType::new(DataType::Boolean, false),
            ));
            types.push(FunctionArgumentType::Value(boolean(false)));
        }
        nodes.push(StaticExprNode::new_value_typed(
            StaticExprKind::Case {
                has_case_expr: simple,
                has_else_expr: has_else,
                children: (1..count + 1).map(ProgramExprId::new).collect(),
            },
            novarocks_functions::FunctionValueType::new(DataType::Boolean, false),
        ));
        types.push(FunctionArgumentType::Value(boolean(true)));
        let (calls, types) = assembled(
            nodes,
            types,
            ProgramExprId::new(0),
            boolean(false),
            EvaluationDemand::Value,
        );
        assert!(matches!(
            ProgramTypedExpressions::try_new(calls, types, &Control),
            Err(ProgramExpressionTypeError::TypeMismatch)
        ));
    }
}

struct Trace {
    at: Option<usize>,
    cause: CompileControlError,
    seen: Mutex<Vec<u32>>,
}
impl PureCompileControl for Trace {
    fn checkpoint(&self, _: CompilePhase, work: u32) -> Result<(), CompileControlError> {
        assert!(work <= 256);
        let mut seen = self.seen.lock().unwrap();
        let at = seen.len();
        seen.push(work);
        if self.at == Some(at) {
            Err(self.cause.clone())
        } else {
            Ok(())
        }
    }
}
#[test]
fn wide_actual_case_keeps_original_control_at_entry_positive_quantum_and_publication_tail() {
    let mut children = Vec::new();
    for _ in 0..320 {
        children.extend([boolean(true), boolean(false)]);
    }
    children.push(boolean(false));
    let (calls, types) = fixture(
        false,
        true,
        children,
        boolean(true),
        EvaluationDemand::TruthOnly,
    );
    let success = Trace {
        at: None,
        cause: CompileControlError::Cancelled,
        seen: Mutex::new(Vec::new()),
    };
    ProgramTypedExpressions::try_new(calls.clone(), types.clone(), &success).unwrap();
    let expected = success.seen.into_inner().unwrap();
    let quantum = expected
        .iter()
        .position(|work| *work == 256)
        .expect("actual type traversal quantum");
    assert!(expected.last().is_some_and(|work| *work > 0));
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in [0, quantum, expected.len() - 1] {
            let control = Trace {
                at: Some(at),
                cause: cause.clone(),
                seen: Mutex::new(Vec::new()),
            };
            assert!(
                matches!(ProgramTypedExpressions::try_new(calls.clone(), types.clone(), &control),
                Err(ProgramExpressionTypeError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.seen.lock().unwrap(), expected[..=at]);
        }
    }
}
