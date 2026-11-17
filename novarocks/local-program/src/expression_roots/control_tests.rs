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

//! Actual program/root correspondence; these tests do not establish expression
//! type, function-owner, effect or runtime evaluation proofs. Intrinsic shape
//! and ordered-child correspondence are checked against actual definitions.

use super::*;
use crate::{
    BindingRequirement, BindingRequirements, CompileProfile, ControlShape, KernelAbiVersion,
    ProgramEvaluationRegion, ProgramExpressionUse, ProgramNode, StaticExprKind, StaticExprNode,
    StaticLayout, StaticSinkProgram, StaticStreamBranch, StaticValues, StaticWriterProjection,
};
use arrow_array::{BooleanArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use novarocks_connector_contract::WriteTargetOrdinal;
use novarocks_execution_contract::DataStreamPartitionType;
use novarocks_type_contract::{EvaluationRegionId, ExpressionEffectContext};
use novarocks_types::SlotId;
use std::{num::NonZeroUsize, sync::Mutex};

#[derive(Default)]
struct Control {
    failure: Option<(u32, CompileControlError)>,
    observations: Mutex<Vec<(CompilePhase, u32)>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        self.observations.lock().unwrap().push((phase, units));
        if let Some((at, failure)) = self.failure.clone()
            && at == units
        {
            return Err(failure);
        }
        Ok(())
    }
}

fn expressions(kinds: Vec<StaticExprKind>, types: Vec<DataType>) -> Arc<ImmutableExpressions> {
    assert_eq!(kinds.len(), types.len());
    Arc::new(
        crate::test_support::arena(
            kinds
                .into_iter()
                .zip(types)
                .map(|(kind, ty)| {
                    StaticExprNode::new_value_typed(
                        kind,
                        novarocks_functions::FunctionValueType::new(ty, false),
                    )
                })
                .collect(),
        )
        .unwrap(),
    )
}
fn bool_arena() -> Arc<ImmutableExpressions> {
    expressions(
        vec![
            crate::test_support::boolean(true),
            crate::test_support::boolean(false),
        ],
        vec![DataType::Boolean; 2],
    )
}
fn bool_values() -> (StaticValues, StaticLayout) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "source",
        DataType::Boolean,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(BooleanArray::from(vec![Some(true), None]))],
    )
    .unwrap();
    let layout = StaticLayout::try_new(schema, Arc::from([SlotId::new(1)])).unwrap();
    (
        StaticValues::try_new(batch, layout.clone()).unwrap(),
        layout,
    )
}
fn output_layout(count: usize, ty: DataType) -> StaticLayout {
    StaticLayout::try_new(
        Arc::new(Schema::new(
            (0..count)
                .map(|index| Field::new(format!("output_{index}"), ty.clone(), true))
                .collect::<Vec<_>>(),
        )),
        (0..count)
            .map(|index| SlotId::new(u32::try_from(index + 1).unwrap()))
            .collect::<Vec<_>>()
            .into(),
    )
    .unwrap()
}
fn profile(layout: &StaticLayout) -> CompileProfile {
    CompileProfile::new(
        NonZeroUsize::new(1).unwrap(),
        None,
        layout.identity().unwrap(),
        KernelAbiVersion::CURRENT,
    )
}
fn node_site(node: usize, role: ProgramNodeExpressionRole) -> ProgramExpressionRootSite {
    ProgramExpressionRootSite::Node {
        node: ProgramNodeId::new(node),
        role,
    }
}
fn project_site(expression: u32) -> ProgramExpressionRootSite {
    node_site(1, ProgramNodeExpressionRole::ProjectOutput { expression })
}
fn binding(site: ProgramExpressionRootSite, id: u32) -> ProgramRootUseBinding {
    ProgramRootUseBinding {
        site,
        use_id: ExpressionUseId::new(id),
    }
}
fn invocation(id: u32, definition: usize, demand: EvaluationDemand) -> ProgramExpressionUse {
    ProgramExpressionUse {
        context: ExpressionEffectContext {
            use_id: ExpressionUseId::new(id),
            region: EvaluationRegionId::new(u32::MAX),
            demand,
        },
        definition: ProgramExprId::new(definition),
        control: ControlShape::Eager,
        arguments: Box::default(),
    }
}
fn flow(uses: Vec<ProgramExpressionUse>, definition_count: usize) -> ProgramControlFlow {
    ProgramControlFlow::try_new(
        vec![ProgramEvaluationRegion {
            id: EvaluationRegionId::new(u32::MAX),
            parent: None,
            guard: None,
        }],
        uses,
        definition_count,
        &Control::default(),
    )
    .unwrap()
}
fn main_flow(value: ProgramControlFlow) -> BTreeMap<ProgramExpressionArena, ProgramControlFlow> {
    BTreeMap::from([(ProgramExpressionArena::Main, value)])
}
pub(super) fn project_program(
    arena: Arc<ImmutableExpressions>,
    definitions: Vec<usize>,
    ty: DataType,
) -> LocalProgramGraph {
    let (values, source_layout) = bool_values();
    let layout = output_layout(definitions.len(), ty);
    let slots = layout.slots().to_vec();
    LocalProgramGraph::try_new(
        vec![
            ProgramNode::new(10, ProgramNodeKind::Values { values }, source_layout),
            ProgramNode::new(
                20,
                ProgramNodeKind::Project {
                    input: ProgramNodeId::new(0),
                    is_subordinate: false,
                    validate_final_result_input: false,
                    exprs: definitions.into_iter().map(ProgramExprId::new).collect(),
                    expr_slot_ids: slots,
                    expr_slot_schemas: None,
                    output_indices: None,
                },
                layout.clone(),
            ),
        ],
        ProgramNodeId::new(1),
        arena,
        profile(&layout),
        BindingRequirements::try_new(vec![]).unwrap(),
    )
    .unwrap()
}
fn mixed_program(arena: Arc<ImmutableExpressions>, predicate: usize) -> LocalProgramGraph {
    let (values, layout) = bool_values();
    LocalProgramGraph::try_new(
        vec![
            ProgramNode::new(10, ProgramNodeKind::Values { values }, layout.clone()),
            ProgramNode::new(
                20,
                ProgramNodeKind::Filter {
                    input: ProgramNodeId::new(0),
                    predicates: vec![ProgramExprId::new(predicate)].into_boxed_slice(),
                },
                layout.clone(),
            ),
            ProgramNode::new(
                30,
                ProgramNodeKind::Project {
                    input: ProgramNodeId::new(1),
                    is_subordinate: false,
                    validate_final_result_input: false,
                    exprs: vec![ProgramExprId::new(0)],
                    expr_slot_ids: vec![SlotId::new(1)],
                    expr_slot_schemas: None,
                    output_indices: None,
                },
                layout.clone(),
            ),
        ],
        ProgramNodeId::new(2),
        arena,
        profile(&layout),
        BindingRequirements::try_new(vec![]).unwrap(),
    )
    .unwrap()
}
fn mixed_bindings() -> Vec<ProgramRootUseBinding> {
    vec![
        binding(
            node_site(
                1,
                ProgramNodeExpressionRole::FilterPredicate { predicate: 0 },
            ),
            0,
        ),
        binding(
            node_site(
                2,
                ProgramNodeExpressionRole::ProjectOutput { expression: 0 },
            ),
            u32::MAX,
        ),
    ]
}
fn mixed_flow(predicate: usize, predicate_demand: EvaluationDemand) -> ProgramControlFlow {
    flow(
        vec![
            invocation(0, predicate, predicate_demand),
            invocation(u32::MAX, 0, EvaluationDemand::Value),
        ],
        2,
    )
}

#[test]
fn same_definition_keeps_distinct_truth_and_value_roots_in_owned_snapshot() {
    let arena = bool_arena();
    let original = mixed_program(arena.clone(), 0);
    let expected = original.nodes().len();
    let checked = ProgramRootControlBindings::try_new(
        original,
        main_flow(mixed_flow(0, EvaluationDemand::TruthOnly)),
        mixed_bindings(),
        &Control::default(),
    )
    .unwrap();
    assert_eq!(checked.program().nodes().len(), expected);
    assert!(Arc::ptr_eq(checked.program().expressions(), &arena));
    assert!(Arc::ptr_eq(
        &checked.roots().arenas()[&ProgramExpressionArena::Main],
        &arena
    ));
    let uses = checked.flows()[&ProgramExpressionArena::Main].uses();
    assert_eq!(
        uses[&ExpressionUseId::new(0)].definition,
        uses[&ExpressionUseId::new(u32::MAX)].definition
    );
    assert_eq!(
        uses[&ExpressionUseId::new(0)].context.demand,
        EvaluationDemand::TruthOnly
    );
    assert_eq!(
        uses[&ExpressionUseId::new(u32::MAX)].context.demand,
        EvaluationDemand::Value
    );
    assert_eq!(checked.bindings().len(), 2);
}

#[test]
fn missing_extra_or_foreign_scope_and_root_entries_are_rejected() {
    let program = mixed_program(bool_arena(), 0);
    let expected_flow = mixed_flow(0, EvaluationDemand::TruthOnly);
    let expected_bindings = mixed_bindings();
    for bindings in [vec![], vec![expected_bindings[0]], {
        let mut entries = expected_bindings.clone();
        entries.push(binding(project_site(9), 4));
        entries
    }] {
        assert_eq!(
            ProgramRootControlBindings::try_new(
                program.clone(),
                main_flow(expected_flow.clone()),
                bindings,
                &Control::default()
            )
            .unwrap_err(),
            ProgramRootBindingError::IncompleteCoverage
        );
    }
    assert_eq!(
        ProgramRootControlBindings::try_new(
            program.clone(),
            BTreeMap::new(),
            expected_bindings.clone(),
            &Control::default()
        )
        .unwrap_err(),
        ProgramRootBindingError::IncompleteCoverage
    );
    assert_eq!(
        ProgramRootControlBindings::try_new(
            program.clone(),
            BTreeMap::from([(ProgramExpressionArena::Sink, expected_flow.clone())]),
            expected_bindings.clone(),
            &Control::default()
        )
        .unwrap_err(),
        ProgramRootBindingError::InvalidArena
    );
    assert_eq!(
        ProgramRootControlBindings::try_new(
            program,
            BTreeMap::from([
                (ProgramExpressionArena::Main, expected_flow.clone()),
                (ProgramExpressionArena::Sink, expected_flow)
            ]),
            expected_bindings,
            &Control::default()
        )
        .unwrap_err(),
        ProgramRootBindingError::IncompleteCoverage
    );
}

#[test]
fn wrong_site_demand_definition_and_unknown_use_are_rejected() {
    let program = mixed_program(bool_arena(), 0);
    for candidate in [
        mixed_flow(0, EvaluationDemand::Value),
        mixed_flow(1, EvaluationDemand::TruthOnly),
    ] {
        assert_eq!(
            ProgramRootControlBindings::try_new(
                program.clone(),
                main_flow(candidate),
                mixed_bindings(),
                &Control::default()
            )
            .unwrap_err(),
            ProgramRootBindingError::InvalidRoot
        );
    }
    let mut wrong_site = mixed_bindings();
    wrong_site[0].site = node_site(
        99,
        ProgramNodeExpressionRole::FilterPredicate { predicate: 0 },
    );
    let mut wrong_role = mixed_bindings();
    wrong_role[0].site = node_site(
        1,
        ProgramNodeExpressionRole::ProjectOutput { expression: 0 },
    );
    let mut wrong_use = mixed_bindings();
    wrong_use[0].use_id = ExpressionUseId::new(123);
    for bindings in [wrong_site, wrong_role, wrong_use] {
        assert_eq!(
            ProgramRootControlBindings::try_new(
                program.clone(),
                main_flow(mixed_flow(0, EvaluationDemand::TruthOnly)),
                bindings,
                &Control::default()
            )
            .unwrap_err(),
            ProgramRootBindingError::InvalidRoot
        );
    }
}

#[test]
fn duplicate_site_and_shared_root_use_are_distinct_failures() {
    let program = project_program(bool_arena(), vec![0, 0], DataType::Boolean);
    let two = flow(
        vec![
            invocation(0, 0, EvaluationDemand::Value),
            invocation(1, 0, EvaluationDemand::Value),
        ],
        2,
    );
    assert_eq!(
        ProgramRootControlBindings::try_new(
            program.clone(),
            main_flow(two),
            vec![binding(project_site(0), 0), binding(project_site(0), 1)],
            &Control::default()
        )
        .unwrap_err(),
        ProgramRootBindingError::DuplicateSite
    );
    let one = flow(vec![invocation(0, 0, EvaluationDemand::Value)], 2);
    assert_eq!(
        ProgramRootControlBindings::try_new(
            program,
            main_flow(one),
            vec![binding(project_site(0), 0), binding(project_site(1), 0)],
            &Control::default()
        )
        .unwrap_err(),
        ProgramRootBindingError::SharedRootUse
    );
}

#[test]
fn orphan_flow_root_and_binding_to_nonroot_use_are_rejected() {
    let program = project_program(bool_arena(), vec![0], DataType::Boolean);
    let orphan = flow(
        vec![
            invocation(0, 0, EvaluationDemand::Value),
            invocation(1, 0, EvaluationDemand::Value),
        ],
        2,
    );
    assert_eq!(
        ProgramRootControlBindings::try_new(
            program.clone(),
            main_flow(orphan),
            vec![binding(project_site(0), 0)],
            &Control::default()
        )
        .unwrap_err(),
        ProgramRootBindingError::IncompleteCoverage
    );
    let mut owner = invocation(0, 1, EvaluationDemand::Value);
    let program = project_program(
        expressions(
            vec![
                crate::test_support::boolean(true),
                StaticExprKind::Not(ProgramExprId::new(0)),
            ],
            vec![DataType::Boolean; 2],
        ),
        vec![0],
        DataType::Boolean,
    );
    owner.arguments = vec![ExpressionUseId::new(1)].into_boxed_slice();
    let child = flow(vec![owner, invocation(1, 0, EvaluationDemand::Value)], 2);
    assert_eq!(
        ProgramRootControlBindings::try_new(
            program,
            main_flow(child),
            vec![binding(project_site(0), 1)],
            &Control::default()
        )
        .unwrap_err(),
        ProgramRootBindingError::SharedRootUse
    );
}

#[test]
fn definition_membership_checks_child_uses_against_actual_arena() {
    let program = project_program(
        expressions(
            vec![
                crate::test_support::boolean(true),
                StaticExprKind::Not(ProgramExprId::new(0)),
            ],
            vec![DataType::Boolean; 2],
        ),
        vec![1],
        DataType::Boolean,
    );
    let mut owner = invocation(0, 1, EvaluationDemand::Value);
    owner.arguments = vec![ExpressionUseId::new(1)].into_boxed_slice();
    // The standalone flow's advertised count admits definition 2. The actual
    // program has only definitions 0 and 1, and the bad use is not a root.
    let oversized = flow(vec![owner, invocation(1, 2, EvaluationDemand::Value)], 3);
    assert_eq!(
        ProgramRootControlBindings::try_new(
            program,
            main_flow(oversized),
            vec![binding(project_site(0), 0)],
            &Control::default()
        )
        .unwrap_err(),
        ProgramRootBindingError::InvalidDefinition
    );
}

fn intrinsic_snapshot(
    kind: StaticExprKind,
    shape: ControlShape,
    children: &[usize],
) -> Result<ProgramRootControlBindings, ProgramRootBindingError> {
    let arena = expressions(
        vec![
            crate::test_support::boolean(true),
            crate::test_support::boolean(false),
            kind,
        ],
        vec![DataType::Boolean; 3],
    );
    let mut root = invocation(10, 2, EvaluationDemand::Value);
    root.control = shape;
    root.arguments = (0..children.len())
        .map(|ordinal| ExpressionUseId::new(ordinal as u32))
        .collect();
    let uses = std::iter::once(root)
        .chain(children.iter().enumerate().map(|(ordinal, definition)| {
            invocation(ordinal as u32, *definition, EvaluationDemand::Value)
        }))
        .collect();
    ProgramRootControlBindings::try_new(
        project_program(arena, vec![2], DataType::Boolean),
        main_flow(flow(uses, 3)),
        vec![binding(project_site(0), 10)],
        &Control::default(),
    )
}

#[test]
fn actual_intrinsic_shape_and_zero_child_leaves_cannot_be_relabelled() {
    for kind in [
        crate::test_support::boolean(true),
        StaticExprKind::SlotId(SlotId::new(1)),
    ] {
        intrinsic_snapshot(kind.clone(), ControlShape::Eager, &[]).unwrap();
        assert_eq!(
            intrinsic_snapshot(kind.clone(), ControlShape::Eager, &[0]).unwrap_err(),
            ProgramRootBindingError::WrongArguments
        );
        assert_eq!(
            intrinsic_snapshot(kind, ControlShape::Conjunction, &[0]).unwrap_err(),
            ProgramRootBindingError::WrongControl
        );
    }
    for (kind, expected) in [
        (
            StaticExprKind::NaryAnd {
                args: vec![ProgramExprId::new(0), ProgramExprId::new(1)],
            },
            ControlShape::Conjunction,
        ),
        (
            StaticExprKind::NaryOr {
                args: vec![ProgramExprId::new(0), ProgramExprId::new(1)],
            },
            ControlShape::Disjunction,
        ),
    ] {
        intrinsic_snapshot(kind.clone(), expected, &[0, 1]).unwrap();
        assert_eq!(
            intrinsic_snapshot(kind, ControlShape::Eager, &[0, 1]).unwrap_err(),
            ProgramRootBindingError::WrongControl
        );
    }
    for kind in [
        StaticExprKind::Not(ProgramExprId::new(0)),
        StaticExprKind::IsNull(ProgramExprId::new(0)),
        StaticExprKind::Not(ProgramExprId::new(0)),
    ] {
        intrinsic_snapshot(kind.clone(), ControlShape::Eager, &[0]).unwrap();
        assert_eq!(
            intrinsic_snapshot(kind, ControlShape::Conjunction, &[0]).unwrap_err(),
            ProgramRootBindingError::WrongControl
        );
    }
}

#[test]
fn actual_ordered_primitive_children_are_preserved() {
    let kind = StaticExprKind::Eq(ProgramExprId::new(0), ProgramExprId::new(1));
    intrinsic_snapshot(kind.clone(), ControlShape::Eager, &[0, 1]).unwrap();
    assert_eq!(
        intrinsic_snapshot(kind.clone(), ControlShape::Eager, &[1, 0]).unwrap_err(),
        ProgramRootBindingError::WrongArguments
    );
    assert_eq!(
        intrinsic_snapshot(kind, ControlShape::Eager, &[0]).unwrap_err(),
        ProgramRootBindingError::WrongArguments
    );
}

#[test]
fn malformed_case_source_arity_cannot_hide_behind_generic_eager_flow() {
    for (has_case_expr, has_else_expr, children) in [
        (false, false, vec![ProgramExprId::new(0)]),
        (
            true,
            true,
            vec![ProgramExprId::new(0), ProgramExprId::new(1)],
        ),
    ] {
        assert_eq!(
            intrinsic_snapshot(
                StaticExprKind::Case {
                    has_case_expr,
                    has_else_expr,
                    children
                },
                ControlShape::Eager,
                &[]
            )
            .unwrap_err(),
            ProgramRootBindingError::WrongArguments
        );
    }
    assert_eq!(
        intrinsic_snapshot(
            StaticExprKind::Case {
                has_case_expr: false,
                has_else_expr: false,
                children: vec![ProgramExprId::new(0), ProgramExprId::new(1)]
            },
            ControlShape::Eager,
            &[0, 1]
        )
        .unwrap_err(),
        ProgramRootBindingError::WrongControl
    );
}

#[test]
fn guarded_case_flags_and_source_order_match_actual_flat_children() {
    for simple in [false, true] {
        for has_else in [false, true] {
            let shape = ControlShape::Case {
                simple,
                arms: 1,
                has_else,
            };
            let definitions: Vec<usize> = std::iter::repeat_n(0, usize::from(simple))
                .chain([0, 1])
                .chain(std::iter::repeat_n(1, usize::from(has_else)))
                .collect();
            let arena = expressions(
                vec![
                    crate::test_support::boolean(true),
                    crate::test_support::boolean(false),
                    StaticExprKind::Case {
                        has_case_expr: simple,
                        has_else_expr: has_else,
                        children: definitions
                            .iter()
                            .map(|definition| ProgramExprId::new(*definition))
                            .collect(),
                    },
                ],
                vec![DataType::Boolean; 3],
            );
            let mut root = invocation(10, 2, EvaluationDemand::Value);
            root.context.region = EvaluationRegionId::new(0);
            root.control = shape;
            root.arguments = (0..definitions.len())
                .map(|ordinal| ExpressionUseId::new(ordinal as u32))
                .collect();
            let mut domains = vec![ProgramEvaluationRegion {
                id: EvaluationRegionId::new(0),
                parent: None,
                guard: None,
            }];
            let mut uses = vec![root];
            for (ordinal, definition) in definitions.iter().enumerate() {
                let (demand, guard) = novarocks_type_contract::control_argument_semantics(
                    shape,
                    definitions.len(),
                    ordinal,
                    EvaluationDemand::Value,
                )
                .unwrap();
                let domain = if let Some(kind) = guard {
                    let id = EvaluationRegionId::new(ordinal as u32 + 1);
                    domains.push(ProgramEvaluationRegion {
                        id,
                        parent: Some(EvaluationRegionId::new(0)),
                        guard: Some(novarocks_type_contract::RegionGuard {
                            owner: ExpressionUseId::new(10),
                            kind,
                        }),
                    });
                    id
                } else {
                    EvaluationRegionId::new(0)
                };
                let mut child = invocation(ordinal as u32, *definition, demand);
                child.context.region = domain;
                uses.push(child);
            }
            let valid =
                ProgramControlFlow::try_new(domains.clone(), uses.clone(), 3, &Control::default())
                    .unwrap();
            let program = project_program(arena, vec![2], DataType::Boolean);
            ProgramRootControlBindings::try_new(
                program.clone(),
                main_flow(valid),
                vec![binding(project_site(0), 10)],
                &Control::default(),
            )
            .unwrap();
            // Keep real guards/demand and change only the THEN source identity.
            uses[usize::from(simple) + 2].definition = ProgramExprId::new(0);
            let wrong = ProgramControlFlow::try_new(domains, uses, 3, &Control::default()).unwrap();
            assert_eq!(
                ProgramRootControlBindings::try_new(
                    program,
                    main_flow(wrong),
                    vec![binding(project_site(0), 10)],
                    &Control::default()
                )
                .unwrap_err(),
                ProgramRootBindingError::WrongArguments
            );
        }
    }
}

#[test]
fn equally_sized_program_with_same_arena_cannot_reuse_changed_root_bindings() {
    let arena = bool_arena();
    let first = mixed_program(arena.clone(), 0);
    let changed = mixed_program(arena, 1);
    assert_eq!(first.nodes().len(), changed.nodes().len());
    assert!(Arc::ptr_eq(first.expressions(), changed.expressions()));
    let checked = ProgramRootControlBindings::try_new(
        first,
        main_flow(mixed_flow(0, EvaluationDemand::TruthOnly)),
        mixed_bindings(),
        &Control::default(),
    )
    .unwrap();
    assert_eq!(
        ProgramRootControlBindings::try_new(
            changed.clone(),
            checked.flows().clone(),
            mixed_bindings(),
            &Control::default()
        )
        .unwrap_err(),
        ProgramRootBindingError::InvalidRoot
    );
    let rebound = ProgramRootControlBindings::try_new(
        changed,
        main_flow(mixed_flow(1, EvaluationDemand::TruthOnly)),
        mixed_bindings(),
        &Control::default(),
    )
    .unwrap();
    assert_eq!(
        rebound.roots().sites()[&node_site(
            1,
            ProgramNodeExpressionRole::FilterPredicate { predicate: 0 }
        )]
            .definition,
        ProgramExprId::new(1)
    );
}

fn three_scope_program(
    arenas: [Arc<ImmutableExpressions>; 3],
    definition: usize,
    ty: DataType,
) -> LocalProgramGraph {
    let (values, source_layout) = bool_values();
    let layout = output_layout(1, ty);
    let branch = StaticStreamBranch::try_new(
        40,
        DataStreamPartitionType::HashPartitioned,
        vec![ProgramExprId::new(definition)],
        vec![SlotId::new(1)],
        None,
        crate::StaticStreamContract {
            role: crate::ProgramOutputRole::Relational,
            delivery: novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
            routing: crate::StaticStreamRouting::Ordinary,
        },
    )
    .unwrap();
    let sink = StaticSinkProgram::try_data_stream(branch, arenas[2].clone()).unwrap();
    LocalProgramGraph::try_new_with_sink(
        vec![
            ProgramNode::new(10, ProgramNodeKind::Values { values }, source_layout),
            ProgramNode::new(
                20,
                ProgramNodeKind::Project {
                    input: ProgramNodeId::new(0),
                    is_subordinate: false,
                    validate_final_result_input: false,
                    exprs: vec![ProgramExprId::new(definition)],
                    expr_slot_ids: vec![SlotId::new(1)],
                    expr_slot_schemas: None,
                    output_indices: None,
                },
                layout.clone(),
            ),
            ProgramNode::new(
                30,
                ProgramNodeKind::TableWriter {
                    input: ProgramNodeId::new(1),
                    target: WriteTargetOrdinal::try_new(0).unwrap(),
                    expected_layout: layout.clone(),
                    projection: StaticWriterProjection {
                        storage_types: vec![None],
                        arena: arenas[1].clone(),
                        expressions: vec![ProgramExprId::new(definition)],
                        layout: layout.clone(),
                    },
                    writer_multiplex_layout: layout.clone(),
                    partial_aggregates: vec![],
                    partial_preparation: None,
                },
                layout.clone(),
            ),
        ],
        ProgramNodeId::new(2),
        arenas[0].clone(),
        profile(&layout),
        BindingRequirements::try_new(vec![
            BindingRequirement::TableWriter {
                node: ProgramNodeId::new(2),
                layout: layout.clone(),
            },
            BindingRequirement::ExchangeOutput {
                role: crate::ProgramOutputRole::Relational,
                delivery: novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
                branch: 0,
                layout,
            },
        ])
        .unwrap(),
        Some(sink),
    )
    .unwrap()
}
fn scopes() -> [ProgramExpressionArena; 3] {
    [
        ProgramExpressionArena::Main,
        ProgramExpressionArena::WriterProjection(ProgramNodeId::new(2)),
        ProgramExpressionArena::Sink,
    ]
}
fn three_bindings(use_id: u32) -> Vec<ProgramRootUseBinding> {
    vec![
        binding(project_site(0), use_id),
        binding(
            ProgramExpressionRootSite::WriterProjection {
                node: ProgramNodeId::new(2),
                expression: 0,
            },
            use_id,
        ),
        binding(
            ProgramExpressionRootSite::SinkPartition { branch: 0, key: 0 },
            use_id,
        ),
    ]
}

#[test]
fn equal_ids_across_main_writer_and_sink_are_independent_occurrences() {
    let arenas = [bool_arena(), bool_arena(), bool_arena()];
    let program = three_scope_program(arenas.clone(), 0, DataType::Boolean);
    let leaf = flow(vec![invocation(0, 0, EvaluationDemand::Value)], 2);
    let checked = ProgramRootControlBindings::try_new(
        program,
        scopes()
            .into_iter()
            .map(|scope| (scope, leaf.clone()))
            .collect(),
        three_bindings(0),
        &Control::default(),
    )
    .unwrap();
    assert_eq!(checked.bindings().len(), 3);
    for (scope, arena) in scopes().into_iter().zip(&arenas) {
        assert!(Arc::ptr_eq(&checked.roots().arenas()[&scope], arena));
        assert_eq!(
            checked.flows()[&scope].root_use_ids(),
            &[ExpressionUseId::new(0)]
        );
    }
}

#[test]
fn shared_backing_does_not_share_entry_use_or_hide_foreign_writer_scope() {
    let arena = bool_arena();
    let program = three_scope_program([arena.clone(), arena.clone(), arena], 0, DataType::Boolean);
    let leaf = flow(vec![invocation(0, 0, EvaluationDemand::Value)], 2);
    let mut flows: BTreeMap<_, _> = scopes()
        .into_iter()
        .map(|scope| (scope, leaf.clone()))
        .collect();
    let checked = ProgramRootControlBindings::try_new(
        program.clone(),
        flows.clone(),
        three_bindings(0),
        &Control::default(),
    )
    .unwrap();
    assert_eq!(checked.roots().arenas().len(), 3);
    flows.remove(&scopes()[1]);
    flows.insert(
        ProgramExpressionArena::WriterProjection(ProgramNodeId::new(1)),
        leaf,
    );
    assert_eq!(
        ProgramRootControlBindings::try_new(program, flows, three_bindings(0), &Control::default())
            .unwrap_err(),
        ProgramRootBindingError::InvalidArena
    );
}

fn array_arena(children: usize) -> (Arc<ImmutableExpressions>, DataType) {
    let ty = DataType::Boolean;
    let arena = expressions(
        vec![
            crate::test_support::boolean(true),
            StaticExprKind::NaryAnd {
                args: vec![ProgramExprId::new(0); children],
            },
        ],
        vec![DataType::Boolean, ty.clone()],
    );
    (arena, ty)
}
fn array_flow(children: usize) -> ProgramControlFlow {
    let mut root = invocation(u32::MAX, 1, EvaluationDemand::Value);
    root.control = ControlShape::Conjunction;
    root.arguments = (0..children)
        .map(|id| ExpressionUseId::new(u32::try_from(id).unwrap()))
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let uses = std::iter::once(root)
        .chain(
            (0..children)
                .map(|id| invocation(u32::try_from(id).unwrap(), 0, EvaluationDemand::Value)),
        )
        .collect();
    flow(uses, 2)
}

#[test]
fn global_reference_budget_applies_to_complete_individually_legal_scope_forests() {
    let children = MAX_CONTROL_USE_REFERENCES / 3 + 1;
    let (arena, ty) = array_arena(children);
    let program = three_scope_program([arena.clone(), arena.clone(), arena], 1, ty);
    let forest = array_flow(children);
    assert_eq!(forest.root_use_ids(), &[ExpressionUseId::new(u32::MAX)]);
    assert!(forest.uses().len() + children <= MAX_CONTROL_USE_REFERENCES);
    assert!(forest.uses().len() * 3 > MAX_CONTROL_USE_REFERENCES);
    assert_eq!(
        ProgramRootControlBindings::try_new(
            program,
            scopes()
                .into_iter()
                .map(|scope| (scope, forest.clone()))
                .collect(),
            three_bindings(u32::MAX),
            &Control::default()
        )
        .unwrap_err(),
        ProgramRootBindingError::TooManyItems
    );
}

fn two_scope_array_program(
    main_children: usize,
    writer_children: usize,
) -> (
    LocalProgramGraph,
    BTreeMap<ProgramExpressionArena, ProgramControlFlow>,
) {
    let (main, ty) = array_arena(main_children);
    let (writer, writer_ty) = array_arena(writer_children);
    assert_eq!(ty, writer_ty);
    let source = three_scope_program([main, writer.clone(), writer], 1, ty);
    let writer_layout = source.nodes()[2].output_layout().clone();
    let program = LocalProgramGraph::try_new(
        source.nodes().to_vec(),
        source.root(),
        source.expressions().clone(),
        source.profile(),
        BindingRequirements::try_new(vec![BindingRequirement::TableWriter {
            node: ProgramNodeId::new(2),
            layout: writer_layout,
        }])
        .unwrap(),
    )
    .unwrap();
    let flows = BTreeMap::from([
        (ProgramExpressionArena::Main, array_flow(main_children)),
        (
            ProgramExpressionArena::WriterProjection(ProgramNodeId::new(2)),
            array_flow(writer_children),
        ),
    ]);
    (program, flows)
}

#[test]
fn global_reference_budget_accepts_exact_limit_and_rejects_valid_one_over() {
    let (program, flows) = two_scope_array_program(16_383, 16_384);
    assert_eq!(
        flows
            .values()
            .map(ProgramControlFlow::use_reference_count)
            .sum::<usize>(),
        MAX_CONTROL_USE_REFERENCES
    );
    let two_bindings = three_bindings(u32::MAX).into_iter().take(2).collect();
    let checked =
        ProgramRootControlBindings::try_new(program, flows, two_bindings, &Control::default())
            .unwrap();
    assert_eq!(checked.bindings().len(), 2);

    let counts = [10_922, 10_922, 10_923];
    let (main, ty) = array_arena(counts[0]);
    let (writer, _) = array_arena(counts[1]);
    let (sink, _) = array_arena(counts[2]);
    let program = three_scope_program([main, writer, sink], 1, ty);
    let flows: BTreeMap<_, _> = scopes()
        .into_iter()
        .zip(counts)
        .map(|(scope, count)| (scope, array_flow(count)))
        .collect();
    assert_eq!(
        flows
            .values()
            .map(ProgramControlFlow::use_reference_count)
            .sum::<usize>(),
        MAX_CONTROL_USE_REFERENCES + 1
    );
    assert!(
        flows
            .values()
            .map(|value| value.uses().len())
            .sum::<usize>()
            < MAX_CONTROL_USE_REFERENCES
    );
    assert_eq!(
        ProgramRootControlBindings::try_new(
            program,
            flows,
            three_bindings(u32::MAX),
            &Control::default()
        )
        .unwrap_err(),
        ProgramRootBindingError::TooManyItems
    );
}

#[test]
fn combined_scope_references_are_bounded_even_when_use_and_edge_totals_each_fit() {
    let (program, flows) = two_scope_array_program(20_000, 20_000);
    assert!(
        flows
            .values()
            .all(|value| value.use_reference_count() <= MAX_CONTROL_USE_REFERENCES)
    );
    assert!(
        flows
            .values()
            .map(|value| value.uses().len())
            .sum::<usize>()
            < MAX_CONTROL_USE_REFERENCES
    );
    const { assert!(40_000 < MAX_CONTROL_USE_REFERENCES) };
    assert!(
        flows
            .values()
            .map(ProgramControlFlow::use_reference_count)
            .sum::<usize>()
            > MAX_CONTROL_USE_REFERENCES
    );
    let bindings = three_bindings(u32::MAX).into_iter().take(2).collect();
    assert_eq!(
        ProgramRootControlBindings::try_new(program, flows, bindings, &Control::default())
            .unwrap_err(),
        ProgramRootBindingError::TooManyItems
    );
}

#[test]
fn compile_control_failures_at_entry_preserve_typed_category() {
    for failure in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let control = Control {
            failure: Some((0, failure.clone())),
            ..Control::default()
        };
        let program = project_program(bool_arena(), vec![0], DataType::Boolean);
        assert_eq!(
            ProgramRootControlBindings::try_new(
                program,
                main_flow(flow(vec![invocation(0, 0, EvaluationDemand::Value)], 2)),
                vec![binding(project_site(0), 0)],
                &control
            )
            .unwrap_err(),
            ProgramRootBindingError::Control(failure)
        );
        assert_eq!(
            *control.observations.lock().unwrap(),
            vec![(CompilePhase::LowerProgram, 0)]
        );
    }
}

#[test]
fn root_collection_and_flow_binding_observe_control_at_256_work() {
    for failure in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let control = Control {
            failure: Some((256, failure.clone())),
            ..Control::default()
        };
        let program = project_program(bool_arena(), vec![0; 300], DataType::Boolean);
        let roots = flow(
            (0..300)
                .map(|id| invocation(id, 0, EvaluationDemand::Value))
                .collect(),
            2,
        );
        let bindings = (0..300).map(|id| binding(project_site(id), id)).collect();
        assert_eq!(
            ProgramRootControlBindings::try_new(program, main_flow(roots), bindings, &control)
                .unwrap_err(),
            ProgramRootBindingError::Roots(ProgramExpressionRootError::Control(failure.clone()))
        );
        assert_eq!(
            control.observations.lock().unwrap().last().copied(),
            Some((CompilePhase::LowerProgram, 256))
        );

        let control = Control {
            failure: Some((256, failure.clone())),
            ..Control::default()
        };
        let (arena, ty) = array_arena(300);
        let program = project_program(arena, vec![1], ty);
        assert_eq!(
            ProgramRootControlBindings::try_new(
                program,
                main_flow(array_flow(300)),
                vec![binding(project_site(0), u32::MAX)],
                &control
            )
            .unwrap_err(),
            ProgramRootBindingError::Control(failure)
        );
        let observations = control.observations.lock().unwrap();
        assert_eq!(
            observations.last().copied(),
            Some((CompilePhase::LowerProgram, 256))
        );
        assert!(
            observations
                .iter()
                .all(|(phase, units)| *phase == CompilePhase::LowerProgram && *units <= 256)
        );
    }
}

// The supplied owner is lifetime-only. Its observer records actual public
// requests and locked opaque operation bounds, without manufacturing a grant.
struct CreationHold(Arc<std::sync::atomic::AtomicUsize>);
impl novarocks_type_contract::ValueTypeStorageHold for CreationHold {}
impl Drop for CreationHold {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CreationEvent {
    Layout(usize, usize),
    Opaque(usize),
}
#[derive(Debug, Eq, PartialEq)]
struct CallerRefusal {
    operation: usize,
    original: u64,
}
fn creation_storage(drops: &Arc<std::sync::atomic::AtomicUsize>) -> ValueTypeStorageLifetime {
    ValueTypeStorageLifetime::held(Arc::new(CreationHold(drops.clone())))
}

#[test]
fn root_binding_temporary_set_exits_while_created_binding_map_stays_live() {
    let program = mixed_program(bool_arena(), 0);
    let flows = main_flow(mixed_flow(0, EvaluationDemand::TruthOnly));
    let bindings = mixed_bindings();
    let expected = ProgramRootControlBindings::try_new(
        program.clone(),
        flows.clone(),
        bindings.clone(),
        &Control::default(),
    )
    .unwrap();
    let temporary_drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let retained_drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let temp_events = Arc::new(Mutex::new(Vec::new()));
    let events = temp_events.clone();
    let mut temp_request = move |layout: std::alloc::Layout| -> Result<(), CallerRefusal> {
        events
            .lock()
            .unwrap()
            .push(CreationEvent::Layout(layout.size(), layout.align()));
        Ok(())
    };
    let events = temp_events.clone();
    let mut temp_opaque = move |bytes| -> Result<(), CallerRefusal> {
        events.lock().unwrap().push(CreationEvent::Opaque(bytes));
        Ok(())
    };
    let mut retained_request = |_: std::alloc::Layout| -> Result<(), CallerRefusal> { Ok(()) };
    let mut retained_opaque = |_: usize| -> Result<(), CallerRefusal> { Ok(()) };
    let mut temporary = ValueSchemaCreation::new(
        creation_storage(&temporary_drops),
        &mut temp_request,
        &mut temp_opaque,
    );
    let mut retained = ValueSchemaCreation::new(
        creation_storage(&retained_drops),
        &mut retained_request,
        &mut retained_opaque,
    );
    let snapshot = ProgramRootControlBindings::try_new_with_creator_in(
        program,
        flows,
        bindings,
        &mut temporary,
        &mut retained,
        &Control::default(),
    )
    .unwrap();
    assert_eq!(snapshot.bindings(), expected.bindings());
    assert_eq!(snapshot.roots().sites(), expected.roots().sites());
    assert!(
        temp_events
            .lock()
            .unwrap()
            .iter()
            .all(|event| matches!(event,CreationEvent::Opaque(bytes) if *bytes > 0))
    );
    assert_eq!(temp_events.lock().unwrap().len(), 2);
    drop(temporary);
    assert_eq!(temporary_drops.load(std::sync::atomic::Ordering::SeqCst), 1);
    let late = snapshot.bindings.clone();
    drop(snapshot);
    drop(retained);
    assert_eq!(retained_drops.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(late.len(), 2);
    drop(late);
    assert_eq!(retained_drops.load(std::sync::atomic::Ordering::SeqCst), 1);
    // A scratch refusal is returned before the corresponding Set insertion;
    // no later retained construction request or footer may follow it.
    let rejected = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut request = |_| -> Result<(), CallerRefusal> { Ok(()) };
    let rejection = rejected.clone();
    let mut opaque = move |_| -> Result<(), CallerRefusal> {
        rejection.store(true, std::sync::atomic::Ordering::SeqCst);
        Err(CallerRefusal {
            operation: 0,
            original: 71,
        })
    };
    let mut temporary =
        ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut request, &mut opaque);
    let no_tail = rejected.clone();
    let mut request = move |_| -> Result<(), CallerRefusal> {
        assert!(
            !no_tail.load(std::sync::atomic::Ordering::SeqCst),
            "retained request after scratch refusal"
        );
        Ok(())
    };
    let no_tail = rejected.clone();
    let mut opaque = move |_| -> Result<(), CallerRefusal> {
        assert!(
            !no_tail.load(std::sync::atomic::Ordering::SeqCst),
            "retained opaque request after scratch refusal"
        );
        Ok(())
    };
    let mut retained =
        ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut request, &mut opaque);
    assert!(matches!(
        ProgramRootControlBindings::try_new_with_creator_in(
            mixed_program(bool_arena(), 0),
            main_flow(mixed_flow(0, EvaluationDemand::TruthOnly)),
            mixed_bindings(),
            &mut temporary,
            &mut retained,
            &Control::default()
        ),
        Err(ProgramCreationFailure::Creator(CallerRefusal {
            original: 71,
            ..
        }))
    ));
}
