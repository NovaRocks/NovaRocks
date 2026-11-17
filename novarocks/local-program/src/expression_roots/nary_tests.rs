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

//! Checked local AST/control correspondence only. No Boolean runtime or
//! kernel catalogue acceptance is established by these construction fixtures.
use super::*;
use crate::{
    BindingRequirements, CompileProfile, ExpressionsCompileError, KernelAbiVersion,
    MAX_STATIC_EXPRESSION_DEPTH, ProgramControlFlowError, ProgramEvaluationRegion,
    ProgramExpressionUse, ProgramNode, StaticExprNode, StaticExpressionError,
};
use arrow_schema::DataType;
use novarocks_type_contract::{EvaluationRegionId, ExpressionEffectContext};
use std::{num::NonZeroUsize, sync::Mutex};

#[derive(Default)]
struct Control {
    refuse_units: Option<(u32, CompileControlError)>,
    trace: Mutex<Vec<(CompilePhase, u32)>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        self.trace.lock().unwrap().push((phase, units));
        match self.refuse_units.clone() {
            Some((at, error)) if at == units => Err(error),
            _ => Ok(()),
        }
    }
}
fn kind(and: bool, args: &[usize]) -> StaticExprKind {
    let args = args.iter().copied().map(ProgramExprId::new).collect();
    if and {
        StaticExprKind::NaryAnd { args }
    } else {
        StaticExprKind::NaryOr { args }
    }
}
fn nodes(and: bool, args: &[usize]) -> Vec<StaticExprNode> {
    vec![
        StaticExprNode::new_value_typed(
            crate::test_support::boolean(true),
            novarocks_functions::FunctionValueType::new(DataType::Boolean, false),
        ),
        StaticExprNode::new_value_typed(
            crate::test_support::boolean(false),
            novarocks_functions::FunctionValueType::new(DataType::Boolean, false),
        ),
        StaticExprNode::new_value_typed(
            kind(and, args),
            novarocks_functions::FunctionValueType::new(DataType::Boolean, false),
        ),
    ]
}
fn arena(and: bool, args: &[usize]) -> Arc<ImmutableExpressions> {
    Arc::new(crate::test_support::arena(nodes(and, args)).unwrap())
}
fn shape(and: bool) -> ControlShape {
    if and {
        ControlShape::Conjunction
    } else {
        ControlShape::Disjunction
    }
}
fn invocation(use_id: u32, definition: usize, demand: EvaluationDemand) -> ProgramExpressionUse {
    ProgramExpressionUse {
        context: ExpressionEffectContext {
            use_id: ExpressionUseId::new(use_id),
            region: EvaluationRegionId::new(0),
            demand,
        },
        definition: ProgramExprId::new(definition),
        control: ControlShape::Eager,
        arguments: Box::default(),
    }
}
fn uses(
    shape: ControlShape,
    children: &[usize],
    demand: EvaluationDemand,
) -> Vec<ProgramExpressionUse> {
    let mut root = invocation(u32::MAX, 2, demand);
    root.control = shape;
    root.arguments = (0..children.len())
        .map(|ordinal| ExpressionUseId::new(17 + ordinal as u32 * 3))
        .collect();
    std::iter::once(root)
        .chain(
            children.iter().enumerate().map(|(ordinal, definition)| {
                invocation(17 + ordinal as u32 * 3, *definition, demand)
            }),
        )
        .collect()
}
fn checked_flow(
    shape: ControlShape,
    children: &[usize],
    demand: EvaluationDemand,
) -> ProgramControlFlow {
    ProgramControlFlow::try_new(
        vec![ProgramEvaluationRegion {
            id: EvaluationRegionId::new(0),
            parent: None,
            guard: None,
        }],
        uses(shape, children, demand),
        3,
        &Control::default(),
    )
    .unwrap()
}
fn graph(arena: Arc<ImmutableExpressions>, demand: EvaluationDemand) -> LocalProgramGraph {
    let project = super::control_tests::project_program(arena.clone(), vec![2], DataType::Boolean);
    if demand == EvaluationDemand::Value {
        return project;
    }
    let source = project.nodes()[0].clone();
    let layout = source.output_layout().clone();
    LocalProgramGraph::try_new(
        vec![
            source,
            ProgramNode::new(
                20,
                ProgramNodeKind::Filter {
                    input: ProgramNodeId::new(0),
                    predicates: vec![ProgramExprId::new(2)].into_boxed_slice(),
                },
                layout.clone(),
            ),
        ],
        ProgramNodeId::new(1),
        arena,
        CompileProfile::new(
            NonZeroUsize::new(1).unwrap(),
            None,
            layout.identity().unwrap(),
            KernelAbiVersion::CURRENT,
        ),
        BindingRequirements::try_new(vec![]).unwrap(),
    )
    .unwrap()
}
fn snapshot(
    arena: Arc<ImmutableExpressions>,
    flow: ProgramControlFlow,
    demand: EvaluationDemand,
    control: &dyn PureCompileControl,
) -> Result<ProgramRootControlBindings, ProgramRootBindingError> {
    let role = if demand == EvaluationDemand::Value {
        ProgramNodeExpressionRole::ProjectOutput { expression: 0 }
    } else {
        ProgramNodeExpressionRole::FilterPredicate { predicate: 0 }
    };
    ProgramRootControlBindings::try_new(
        graph(arena, demand),
        BTreeMap::from([(ProgramExpressionArena::Main, flow)]),
        vec![ProgramRootUseBinding {
            site: ProgramExpressionRootSite::Node {
                node: ProgramNodeId::new(1),
                role,
            },
            use_id: ExpressionUseId::new(u32::MAX),
        }],
        control,
    )
}

#[test]
fn actual_nary_one_three_and_320_children_preserve_value_and_truth_only_occurrences() {
    for and in [true, false] {
        for demand in [EvaluationDemand::Value, EvaluationDemand::TruthOnly] {
            for count in [1, 3, 320] {
                let args: Vec<_> = (0..count).map(|ordinal| ordinal % 2).collect();
                let arena = arena(and, &args);
                let checked = snapshot(
                    arena.clone(),
                    checked_flow(shape(and), &args, demand),
                    demand,
                    &Control::default(),
                )
                .unwrap();
                let flow = &checked.flows()[&ProgramExpressionArena::Main];
                let root = &flow.uses()[&ExpressionUseId::new(u32::MAX)];
                assert_eq!(root.control, shape(and));
                assert_eq!(root.context.demand, demand);
                assert_eq!(root.arguments.len(), count);
                let actual_args = match arena.node(ProgramExprId::new(2)).unwrap().kind() {
                    StaticExprKind::NaryAnd { args } | StaticExprKind::NaryOr { args } => args,
                    _ => panic!("expected the actual nary definition"),
                };
                assert_eq!(
                    actual_args,
                    &args
                        .iter()
                        .copied()
                        .map(ProgramExprId::new)
                        .collect::<Vec<_>>()
                );
                let mut seen = BTreeSet::new();
                for (ordinal, use_id) in root.arguments.iter().enumerate() {
                    assert!(seen.insert(*use_id));
                    assert_eq!(flow.uses()[use_id].definition, actual_args[ordinal]);
                    assert_eq!(flow.uses()[use_id].context.demand, demand);
                    assert_eq!(flow.uses()[use_id].context.region, root.context.region);
                }
            }
        }
    }
}

#[test]
fn nary_same_definition_repeats_keep_distinct_uses_and_reject_shared_invocation() {
    let args = [0, 1, 0];
    snapshot(
        arena(true, &args),
        checked_flow(ControlShape::Conjunction, &args, EvaluationDemand::Value),
        EvaluationDemand::Value,
        &Control::default(),
    )
    .unwrap();
    let mut shared = uses(ControlShape::Conjunction, &args, EvaluationDemand::Value);
    let first_use = shared[0].arguments[0];
    shared[0].arguments[2] = first_use;
    assert_eq!(
        ProgramControlFlow::try_new(
            vec![ProgramEvaluationRegion {
                id: EvaluationRegionId::new(0),
                parent: None,
                guard: None
            }],
            shared,
            3,
            &Control::default(),
        )
        .unwrap_err(),
        ProgramControlFlowError::SharedUse
    );
}

#[test]
fn nary_actual_order_arity_and_control_cannot_be_relabelled_by_a_valid_common_flow() {
    for and in [true, false] {
        let args = [0, 1, 0];
        for children in [vec![1, 0, 0], vec![0, 1], vec![0, 1, 0, 1]] {
            assert_eq!(
                snapshot(
                    arena(and, &args),
                    checked_flow(shape(and), &children, EvaluationDemand::Value),
                    EvaluationDemand::Value,
                    &Control::default()
                )
                .unwrap_err(),
                ProgramRootBindingError::WrongArguments
            );
        }
        for wrong in [ControlShape::Eager, shape(!and)] {
            assert_eq!(
                snapshot(
                    arena(and, &args),
                    checked_flow(wrong, &args, EvaluationDemand::Value),
                    EvaluationDemand::Value,
                    &Control::default()
                )
                .unwrap_err(),
                ProgramRootBindingError::WrongControl
            );
        }
    }
}

#[test]
fn nary_child_demand_and_actual_root_site_demand_are_independently_checked() {
    let args = [0, 1, 0];
    for and in [true, false] {
        let mut wrong = uses(shape(and), &args, EvaluationDemand::TruthOnly);
        wrong[2].context.demand = EvaluationDemand::Value;
        assert_eq!(
            ProgramControlFlow::try_new(
                vec![ProgramEvaluationRegion {
                    id: EvaluationRegionId::new(0),
                    parent: None,
                    guard: None
                }],
                wrong,
                3,
                &Control::default(),
            )
            .unwrap_err(),
            ProgramControlFlowError::InvalidDemand
        );
        let result = snapshot(
            arena(and, &args),
            checked_flow(shape(and), &args, EvaluationDemand::Value),
            EvaluationDemand::TruthOnly,
            &Control::default(),
        );
        assert_eq!(result.unwrap_err(), ProgramRootBindingError::InvalidRoot);
    }
}

#[test]
fn nary_definition_author_rejects_empty_forward_and_dangling_references_without_changing_depth_budget()
 {
    for and in [true, false] {
        for (args, error) in [
            (vec![], StaticExpressionError::InvalidMetadataArity),
            (vec![2], StaticExpressionError::InvalidReference),
            (vec![usize::MAX], StaticExpressionError::InvalidReference),
        ] {
            assert_eq!(
                crate::test_support::arena(nodes(and, &args)).unwrap_err(),
                error
            );
        }
        arena(and, &vec![0; 320]);
        let nested = |count: usize| {
            let mut nodes = vec![StaticExprNode::new_value_typed(
                crate::test_support::boolean(true),
                novarocks_functions::FunctionValueType::new(DataType::Boolean, false),
            )];
            for ordinal in 1..count {
                nodes.push(StaticExprNode::new_value_typed(
                    kind(and, &[ordinal - 1]),
                    novarocks_functions::FunctionValueType::new(DataType::Boolean, false),
                ));
            }
            crate::test_support::arena(nodes)
        };
        nested(MAX_STATIC_EXPRESSION_DEPTH).unwrap();
        assert_eq!(
            nested(MAX_STATIC_EXPRESSION_DEPTH + 1).unwrap_err(),
            StaticExpressionError::TooDeep
        );
    }
}

#[test]
fn nary_definition_and_root_correspondence_keep_original_entry_and_256_control_failures() {
    let args = vec![0; 320];
    for failure in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for units in [0, 256] {
            let control = Control {
                refuse_units: Some((units, failure.clone())),
                ..Control::default()
            };
            assert_eq!(
                ImmutableExpressions::try_new_for_compile(nodes(true, &args), &control)
                    .unwrap_err(),
                ExpressionsCompileError::Control(failure.clone())
            );
            assert_eq!(
                control.trace.lock().unwrap().last().copied(),
                Some((CompilePhase::LowerProgram, units))
            );
            let control = Control {
                refuse_units: Some((units, failure.clone())),
                ..Control::default()
            };
            let error = snapshot(
                arena(false, &args),
                checked_flow(
                    ControlShape::Disjunction,
                    &args,
                    EvaluationDemand::TruthOnly,
                ),
                EvaluationDemand::TruthOnly,
                &control,
            )
            .unwrap_err();
            assert!(matches!(error, ProgramRootBindingError::Control(cause)
                | ProgramRootBindingError::Roots(ProgramExpressionRootError::Control(cause)) if cause == failure));
            assert_eq!(
                control.trace.lock().unwrap().last().copied(),
                Some((CompilePhase::LowerProgram, units))
            );
        }
    }
}
