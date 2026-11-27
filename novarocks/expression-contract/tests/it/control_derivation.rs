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

use super::support::*;
use novarocks_expression_contract::*;
use novarocks_failure_contract::{FailureClass, FailureProjection};
use novarocks_type_contract::*;
use std::collections::BTreeMap;

#[test]
fn try_body_has_its_own_region_and_value_demand() {
    let ty = boolean(true);
    let mut builder = BoundExprSetBuilder::new(limits());
    let leaf = builder
        .add(None, ty.clone(), ExprKind::input(ValueId::new(9)))
        .unwrap();
    let function = function(
        "fixture/unit/try",
        vec![FunctionArgumentType::Value(ty.clone())],
        ty.clone(),
    );
    let request = request(&function);
    let root = builder
        .add(
            None,
            ty.clone(),
            ExprKind::FunctionCall {
                function,
                args: Box::new([leaf]),
            },
        )
        .unwrap();
    builder.request(root, request).unwrap();
    builder
        .definition_control(
            root,
            DefinitionControl {
                shape: ControlShape::Try,
                temporal_sources: None,
            },
        )
        .unwrap();
    builder
        .root(root, EvaluationDemand::TruthOnly, RootScope::Port(0))
        .unwrap();
    let control = Control::good();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
    let sealed = builder
        .seal(
            vec![ExprPort {
                values: Box::new([(ValueId::new(9), ty)]),
            }],
            &mut work,
        )
        .unwrap();
    let root_use = sealed.root_use_ids()[0];
    let owner = &sealed.uses()[&root_use];
    let body = &sealed.uses()[&owner.arguments[0]];
    assert_eq!(body.context.demand, EvaluationDemand::Value);
    let scope = &sealed.control_flow().regions()[&body.context.region];
    assert_eq!(scope.parent, Some(owner.context.region));
    assert_eq!(
        scope.guard,
        Some(RegionGuard {
            owner: root_use,
            kind: GuardKind::TryBody,
        }),
    );
}

#[test]
fn shared_if_retains_independent_value_truth_uses_and_hand_checked_guards() {
    let ty = boolean(true);
    let mut builder = BoundExprSetBuilder::new(limits());
    let leaf = builder
        .add(None, ty.clone(), ExprKind::input(ValueId::new(9)))
        .unwrap();
    let mut f = function(
        "fixture/unit/if",
        vec![FunctionArgumentType::Value(ty.clone()); 3],
        ty.clone(),
    );
    f.legacy_metadata = Some(LegacyBindingMetadata {
        volatility: FunctionVolatility::Volatile,
        argument_evaluation: FunctionArgumentEvaluation::Eager,
        failure_behavior: FunctionFailureBehavior::Propagate,
        intrinsic_row_error: FunctionIntrinsicRowError::MayRaise,
        semantic_parameters: Box::default(),
    });
    let request = request(&f);
    let call = builder
        .add(
            None,
            ty.clone(),
            ExprKind::FunctionCall {
                function: f,
                args: Box::new([leaf, leaf, leaf]),
            },
        )
        .unwrap();
    builder.request(call, request).unwrap();
    builder
        .definition_control(
            call,
            DefinitionControl {
                shape: ControlShape::If,
                temporal_sources: None,
            },
        )
        .unwrap();
    builder
        .root(call, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    builder
        .root(call, EvaluationDemand::TruthOnly, RootScope::Port(0))
        .unwrap();
    let c = Control::good();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::LowerProgram).unwrap();
    let sealed = builder
        .seal(
            vec![ExprPort {
                values: Box::new([(ValueId::new(9), ty)]),
            }],
            &mut work,
        )
        .unwrap();
    assert_eq!(sealed.uses().len(), 8);
    assert_eq!(sealed.control_flow().regions().len(), 6);
    for (ordinal, &root) in sealed.root_use_ids().iter().enumerate() {
        let owner = &sealed.uses()[&root];
        let demand = [EvaluationDemand::Value, EvaluationDemand::TruthOnly][ordinal];
        assert_eq!(owner.context.demand, demand);
        assert_eq!(owner.control, ControlShape::If);
        let condition = &sealed.uses()[&owner.arguments[0]];
        assert_eq!(condition.context.region, owner.context.region);
        assert_eq!(condition.context.demand, EvaluationDemand::Value);
        for (index, kind) in [(1, GuardKind::IfThen), (2, GuardKind::IfElse)] {
            let child = &sealed.uses()[&owner.arguments[index]];
            assert_eq!(child.context.demand, EvaluationDemand::Value);
            let region = &sealed.control_flow().regions()[&child.context.region];
            assert_eq!(region.parent, Some(owner.context.region));
            assert_eq!(region.guard, Some(RegionGuard { owner: root, kind }));
        }
    }
    let unit = with_call_claims(sealed, ArgumentControl::If, &mut work).unwrap();
    work.finish().unwrap();
    assert_eq!(unit.claims().len(), 2);
}
#[test]
fn type_only_retains_hidden_function_request_without_inventing_its_control_or_use() {
    let ty = boolean(true);
    let mut b = BoundExprSetBuilder::new(limits());
    let inner = function("fixture/unit/hidden", vec![], ty.clone());
    let inner_request = request(&inner);
    let inner_id = b
        .add(
            None,
            ty.clone(),
            ExprKind::FunctionCall {
                function: inner,
                args: Box::default(),
            },
        )
        .unwrap();
    b.request(inner_id, inner_request).unwrap();
    let outer = function(
        "fixture/unit/type-only",
        vec![FunctionArgumentType::Value(ty.clone())],
        ty.clone(),
    );
    let outer_request = request(&outer);
    let id = b
        .add(
            None,
            ty,
            ExprKind::FunctionCall {
                function: outer,
                args: Box::new([inner_id]),
            },
        )
        .unwrap();
    b.request(id, outer_request).unwrap();
    b.definition_control(
        id,
        DefinitionControl {
            shape: ControlShape::TypeOnly,
            temporal_sources: None,
        },
    )
    .unwrap();
    b.root(id, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    let c = Control::good();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Validate).unwrap();
    let sealed = b.seal(vec![], &mut work).unwrap();
    assert_eq!(sealed.uses().len(), 1);
    assert_eq!(sealed.requests().len(), 2);
    assert!(sealed.uses().values().next().unwrap().arguments.is_empty());
    let unit = with_call_claims(sealed, ArgumentControl::TypeOnly, &mut work).unwrap();
    work.finish().unwrap();
    assert_eq!(unit.definitions().len(), 2);
    assert_eq!(unit.claims().len(), 1);
    let parent_id = unit.root_use(RootOrdinal::new(0)).unwrap();
    let mut parent = unit.control_flow().uses()[&parent_id].clone();
    let child_id = ExpressionUseId::new(u32::MAX);
    parent.arguments = Box::new([child_id]);
    let child = ExpressionInvocation {
        context: ExpressionEffectContext {
            use_id: child_id,
            region: parent.context.region,
            demand: EvaluationDemand::Value,
        },
        definition: inner_id,
        control: ControlShape::Eager,
        arguments: Box::default(),
    };
    assert_eq!(
        ExpressionControlFlow::try_new(
            unit.control_flow().regions().values().copied().collect(),
            vec![parent, child],
            &Definitions(unit.definitions().iter().map(|node| node.id).collect()),
            CompilePhase::Validate,
            &c,
        )
        .unwrap_err(),
        ExpressionControlFlowError::InvalidControlShape
    );
}
struct Definitions(Vec<ExprId>);
impl ExpressionDefinitionMembership<ExprId> for Definitions {
    fn definition_count(&self) -> usize {
        self.0.len()
    }
    fn contains_definition(&self, id: ExprId) -> bool {
        self.0.contains(&id)
    }
}
#[test]
fn neutral_parts_preserve_sparse_non_dfs_use_ids_and_root_specific_port_bindings() {
    let ty = boolean(true);
    let definition = ExprId::new(0);
    let c = Control::good();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
    let flow = ExpressionControlFlow::try_new_in(
        vec![
            ExpressionEvaluationRegion {
                id: EvaluationRegionId::new(7),
                parent: None,
                guard: None,
            },
            ExpressionEvaluationRegion {
                id: EvaluationRegionId::new(81),
                parent: None,
                guard: None,
            },
        ],
        vec![
            ExpressionInvocation {
                context: ExpressionEffectContext {
                    use_id: ExpressionUseId::new(10),
                    region: EvaluationRegionId::new(7),
                    demand: EvaluationDemand::Value,
                },
                definition,
                control: ControlShape::Eager,
                arguments: Box::default(),
            },
            ExpressionInvocation {
                context: ExpressionEffectContext {
                    use_id: ExpressionUseId::new(95),
                    region: EvaluationRegionId::new(81),
                    demand: EvaluationDemand::TruthOnly,
                },
                definition,
                control: ControlShape::Eager,
                arguments: Box::default(),
            },
        ],
        &Definitions(vec![definition]),
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap();
    let parts = UnitParts::new(
        Box::new([
            ExprPort {
                values: Box::new([(ValueId::new(9), ty.clone())]),
            },
            ExprPort {
                values: Box::new([
                    (ValueId::new(8), integer(false)),
                    (ValueId::new(9), ty.clone()),
                ]),
            },
        ]),
        Box::new([ExprNode {
            id: definition,
            lambda_scope: None,
            ty,
            kind: ExprKind::input(ValueId::new(9)),
        }]),
        Box::new([
            ExprRoot {
                definition,
                demand: EvaluationDemand::TruthOnly,
                scope: RootScope::Port(1),
            },
            ExprRoot {
                definition,
                demand: EvaluationDemand::Value,
                scope: RootScope::Port(0),
            },
        ]),
        Box::new([ExpressionUseId::new(95), ExpressionUseId::new(10)]),
        flow,
        BTreeMap::new(),
        BTreeMap::new(),
    );
    let unit = BoundExprSet::try_from_parts_in(parts, limits(), &mut work).unwrap();
    work.finish().unwrap();
    assert_eq!(
        unit.root_use_ids(),
        [ExpressionUseId::new(95), ExpressionUseId::new(10)]
    );
    assert_eq!(unit.roots()[0].scope, RootScope::Port(1));
}
fn repeated_roots() -> BoundExprSetBuilder {
    let mut b = BoundExprSetBuilder::new(limits());
    let id = b
        .add(None, boolean(false), ExprKind::input(ValueId::new(9)))
        .unwrap();
    for _ in 0..320 {
        b.root(id, EvaluationDemand::Value, RootScope::Port(0))
            .unwrap();
    }
    b
}
#[test]
fn actual_320_shared_roots_observe_real_quantums_and_each_control_cause_keeps_its_prefix() {
    let ports = || {
        vec![ExprPort {
            values: Box::new([(ValueId::new(9), boolean(false))]),
        }]
    };
    let c = Control::good();
    let unit = no_calls(repeated_roots(), ports(), &c).unwrap();
    assert_eq!(unit.control_flow().uses().len(), 320);
    assert_eq!(unit.control_flow().regions().len(), 320);
    let trace = c.trace();
    let quantum = trace
        .iter()
        .position(|(_, n)| *n == 256)
        .expect("actual unit work reaches one quantum");
    for index in [0, 1, quantum, trace.len() - 1] {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let c = Control::at(index, cause.clone());
            let error = no_calls(repeated_roots(), ports(), &c).unwrap_err();
            assert!(matches!(error,UnitError::Control(actual) if actual==cause));
            assert_eq!(c.trace(), trace[..=index]);
        }
    }
}
fn dag(levels: usize) -> BoundExprSetBuilder {
    let mut b = BoundExprSetBuilder::new(limits());
    let mut id = b
        .add(None, boolean(false), ExprKind::input(ValueId::new(9)))
        .unwrap();
    for _ in 0..levels {
        id = b
            .add(
                None,
                boolean(false),
                ExprKind::Conjunction {
                    args: Box::new([id, id]),
                },
            )
            .unwrap();
    }
    b.root(id, EvaluationDemand::TruthOnly, RootScope::Port(0))
        .unwrap();
    b
}
#[test]
fn expanded_dag_charges_each_use_and_edge_against_the_original_reference_bound() {
    let ports = || {
        vec![ExprPort {
            values: Box::new([(ValueId::new(9), boolean(false))]),
        }]
    };
    let unit = no_calls(dag(14), ports(), &Control::good()).unwrap();
    assert_eq!(unit.definitions().len(), 15);
    assert_eq!(unit.control_flow().uses().len(), 32767);
    assert_eq!(unit.control_flow().use_reference_count(), 65533);
    let error = no_calls(dag(15), ports(), &Control::good()).unwrap_err();
    assert_eq!(error.failure_fact().class(), FailureClass::Resource);
}

fn assert_guard(
    unit: &BoundExprSet,
    owner: ExpressionUseId,
    ordinal: usize,
    kind: GuardKind,
    demand: EvaluationDemand,
) {
    let invocation = &unit.control_flow().uses()[&owner];
    let child = &unit.control_flow().uses()[&invocation.arguments[ordinal]];
    assert_eq!(child.context.demand, demand);
    let region = &unit.control_flow().regions()[&child.context.region];
    assert_eq!(region.parent, Some(invocation.context.region));
    assert_eq!(region.guard, Some(RegionGuard { owner, kind }));
}
#[test]
fn intrinsic_boolean_and_both_case_shapes_keep_ordered_hand_checked_guards() {
    let mut b = BoundExprSetBuilder::new(limits());
    let ty = boolean(false);
    let a = b
        .add(None, ty.clone(), ExprKind::input(ValueId::new(9)))
        .unwrap();
    let c = b
        .add(None, ty.clone(), ExprKind::input(ValueId::new(8)))
        .unwrap();
    let not = b
        .add(
            None,
            ty.clone(),
            ExprKind::IsNull {
                negated: true,
                expr: a,
            },
        )
        .unwrap();
    let or = b
        .add(
            None,
            ty.clone(),
            ExprKind::Disjunction {
                args: Box::new([a, not]),
            },
        )
        .unwrap();
    let and = b
        .add(
            None,
            ty.clone(),
            ExprKind::Conjunction {
                args: Box::new([or, c]),
            },
        )
        .unwrap();
    for operand in [None, Some(a)] {
        let id = b
            .add(
                None,
                ty.clone(),
                ExprKind::Case {
                    ieee_float_equality: false,
                    operand,
                    when_then: Box::new([(and, a), (c, not)]),
                    else_expr: Some(c),
                },
            )
            .unwrap();
        b.root(id, EvaluationDemand::Value, RootScope::Port(0))
            .unwrap();
    }
    let unit = no_calls(
        b,
        vec![ExprPort {
            values: Box::new([(ValueId::new(9), ty.clone()), (ValueId::new(8), ty)]),
        }],
        &Control::good(),
    )
    .unwrap();
    for (index, &id) in unit.root_use_ids().iter().enumerate() {
        let simple = index == 1;
        let root = &unit.control_flow().uses()[&id];
        assert_eq!(
            root.control,
            ControlShape::Case {
                simple,
                arms: 2,
                has_else: true
            }
        );
        let first = usize::from(simple);
        if simple {
            assert_eq!(
                unit.control_flow().uses()[&root.arguments[0]]
                    .context
                    .region,
                root.context.region
            );
        }
        for arm in 0..2 {
            assert_guard(
                &unit,
                id,
                first + arm * 2,
                GuardKind::CaseWhen { arm: arm as u32 },
                EvaluationDemand::Value,
            );
            assert_guard(
                &unit,
                id,
                first + arm * 2 + 1,
                GuardKind::CaseThen { arm: arm as u32 },
                EvaluationDemand::Value,
            );
        }
        assert_guard(
            &unit,
            id,
            first + 4,
            GuardKind::CaseElse,
            EvaluationDemand::Value,
        );
        let conjunction = &unit.control_flow().uses()[&root.arguments[first]];
        assert_eq!(conjunction.definition, and);
        assert_eq!(conjunction.control, ControlShape::Conjunction);
        let disjunction = &unit.control_flow().uses()[&conjunction.arguments[0]];
        assert_eq!(disjunction.control, ControlShape::Disjunction);
        let negation = &unit.control_flow().uses()[&disjunction.arguments[1]];
        assert_eq!(negation.control, ControlShape::Eager);
        assert_eq!(negation.context.region, disjunction.context.region);
        assert_eq!(
            unit.control_flow().uses()[&negation.arguments[0]]
                .context
                .region,
            negation.context.region
        );
        assert_eq!(
            unit.control_flow().uses()[&conjunction.arguments[1]]
                .context
                .region,
            conjunction.context.region
        );
    }
}
#[test]
fn coalesce_keeps_its_guard_and_nested_eager_calls_keep_the_guarded_region() {
    let ty = integer(true);
    let mut b = BoundExprSetBuilder::new(limits());
    let leaf = b
        .add(None, ty.clone(), ExprKind::input(ValueId::new(9)))
        .unwrap();
    let f = function(
        "fixture/unit/eager",
        vec![FunctionArgumentType::Value(ty.clone())],
        ty.clone(),
    );
    let req = request(&f);
    let eager = b
        .add(
            None,
            ty.clone(),
            ExprKind::FunctionCall {
                function: f,
                args: Box::new([leaf]),
            },
        )
        .unwrap();
    b.request(eager, req).unwrap();
    b.definition_control(
        eager,
        DefinitionControl {
            shape: ControlShape::Eager,
            temporal_sources: None,
        },
    )
    .unwrap();
    let f = function(
        "fixture/unit/coalesce",
        vec![FunctionArgumentType::Value(ty.clone()); 2],
        ty.clone(),
    );
    let req = request(&f);
    let root = b
        .add(
            None,
            ty.clone(),
            ExprKind::FunctionCall {
                function: f,
                args: Box::new([leaf, eager]),
            },
        )
        .unwrap();
    b.request(root, req).unwrap();
    b.definition_control(
        root,
        DefinitionControl {
            shape: ControlShape::Coalesce,
            temporal_sources: None,
        },
    )
    .unwrap();
    b.root(root, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let c = Control::good();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Validate).unwrap();
    let sealed = b
        .seal(
            vec![ExprPort {
                values: Box::new([(ValueId::new(9), ty)]),
            }],
            &mut work,
        )
        .unwrap();
    let claims = sealed
        .uses()
        .iter()
        .filter_map(
            |(&id, u)| match sealed.definition(u.definition).unwrap().kind {
                ExprKind::FunctionCall { .. } => Some((
                    id,
                    claim(
                        u.context,
                        if u.definition == root {
                            ArgumentControl::Coalesce
                        } else {
                            ArgumentControl::Eager
                        },
                    ),
                )),
                _ => None,
            },
        )
        .collect();
    let unit = sealed.finish(claims, &mut work).unwrap();
    work.finish().unwrap();
    let id = unit.root_use_ids()[0];
    let invocation = &unit.control_flow().uses()[&id];
    assert_eq!(
        unit.control_flow().uses()[&invocation.arguments[0]]
            .context
            .region,
        invocation.context.region
    );
    assert_guard(
        &unit,
        id,
        1,
        GuardKind::CoalesceAfterNull { ordinal: 1 },
        EvaluationDemand::Value,
    );
    let eager = &unit.control_flow().uses()[&invocation.arguments[1]];
    assert_eq!(eager.control, ControlShape::Eager);
    assert_eq!(
        unit.control_flow().uses()[&eager.arguments[0]]
            .context
            .region,
        eager.context.region
    );
}
#[test]
fn no_arguments_keeps_static_children_but_does_not_invent_runtime_uses() {
    let ty = integer(false);
    let mut b = BoundExprSetBuilder::new(limits());
    let leaf = b
        .add(None, ty.clone(), ExprKind::input(ValueId::new(9)))
        .unwrap();
    let f = function(
        "fixture/unit/no-arguments",
        vec![FunctionArgumentType::Value(ty.clone())],
        ty.clone(),
    );
    let req = request(&f);
    let root = b
        .add(
            None,
            ty.clone(),
            ExprKind::FunctionCall {
                function: f,
                args: Box::new([leaf]),
            },
        )
        .unwrap();
    b.request(root, req).unwrap();
    b.definition_control(
        root,
        DefinitionControl {
            shape: ControlShape::NoArguments,
            temporal_sources: None,
        },
    )
    .unwrap();
    b.root(root, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let c = Control::good();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Validate).unwrap();
    let sealed = b
        .seal(
            vec![ExprPort {
                values: Box::new([(ValueId::new(9), ty)]),
            }],
            &mut work,
        )
        .unwrap();
    assert_eq!(sealed.uses().len(), 1);
    assert!(sealed.uses().values().next().unwrap().arguments.is_empty());
    with_call_claims(sealed, ArgumentControl::NoArguments, &mut work).unwrap();
    work.finish().unwrap();
}
#[test]
fn neutral_parts_cannot_bind_an_independent_root_inside_another_roots_guard() {
    let mut b = BoundExprSetBuilder::new(limits());
    let leaf = b
        .add(None, boolean(true), ExprKind::input(ValueId::new(9)))
        .unwrap();
    let case = b
        .add(
            None,
            boolean(true),
            ExprKind::Case {
                ieee_float_equality: false,
                operand: None,
                when_then: Box::new([(leaf, leaf)]),
                else_expr: Some(leaf),
            },
        )
        .unwrap();
    b.root(case, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    b.root(leaf, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let unit = no_calls(
        b,
        vec![ExprPort {
            values: Box::new([(ValueId::new(9), boolean(true))]),
        }],
        &Control::good(),
    )
    .unwrap();
    let mut parts = unit.into_parts();
    let second = parts.root_uses[1];
    let first = &parts.control.uses()[&parts.root_uses[0]];
    let guarded = parts.control.uses()[&first.arguments[1]].context.region;
    let uses = parts
        .control
        .uses()
        .values()
        .cloned()
        .map(|mut u| {
            if u.context.use_id == second {
                u.context.region = guarded;
            }
            u
        })
        .collect();
    // Remove the spare root region so the test isolates the root rule.
    let regions = parts
        .control
        .regions()
        .values()
        .filter(|r| r.id != parts.control.uses()[&second].context.region)
        .copied()
        .collect();
    let c = Control::good();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
    parts.control = ExpressionControlFlow::try_new_in(
        regions,
        uses,
        &Definitions(parts.definitions.iter().map(|n| n.id).collect()),
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap();
    assert!(
        matches!(BoundExprSet::try_from_parts_in(parts,limits(),&mut work),Err(UnitError::Invalid {root:Some(ordinal),message,..}) if ordinal.get()==1 && message.contains("guarded"))
    );
}
#[test]
fn neutral_parts_cannot_relabel_the_exact_temporal_source_shape() {
    let ty = integer(false);
    let mut b = BoundExprSetBuilder::new(limits());
    let leaf = b
        .add(None, ty.clone(), ExprKind::input(ValueId::new(9)))
        .unwrap();
    let f = function(
        "fixture/unit/time_to_sec",
        vec![FunctionArgumentType::Value(ty.clone())],
        ty.clone(),
    );
    let req = request(&f);
    let root = b
        .add(
            None,
            ty.clone(),
            ExprKind::FunctionCall {
                function: f,
                args: Box::new([leaf]),
            },
        )
        .unwrap();
    b.request(root, req).unwrap();
    let sources = TemporalSourceDefinitions {
        facts: TemporalSourceFacts::SecondsDirect {
            cast_chain: Box::default(),
        },
        definitions: Box::new([leaf]),
    };
    b.definition_control(
        root,
        DefinitionControl {
            shape: ControlShape::TemporalSource(TemporalSourceShape::SecondsDirect),
            temporal_sources: Some(sources),
        },
    )
    .unwrap();
    b.root(root, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let c = Control::good();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Validate).unwrap();
    let sealed = b
        .seal(
            vec![ExprPort {
                values: Box::new([(ValueId::new(9), ty)]),
            }],
            &mut work,
        )
        .unwrap();
    let id = sealed.root_use_ids()[0];
    let mut declaration = claim(
        sealed.uses()[&id].context,
        ArgumentControl::TemporalSource(TemporalSourceKind::TimeToSec),
    );
    declaration.temporal_source = Some(sealed.temporal_source(id).unwrap().clone());
    let unit = sealed
        .finish(BTreeMap::from([(id, declaration)]), &mut work)
        .unwrap();
    let mut parts = unit.into_parts();
    let uses = parts
        .control
        .uses()
        .values()
        .cloned()
        .map(|mut u| {
            if u.context.use_id == id {
                u.control = ControlShape::TemporalSource(TemporalSourceShape::SecondsRoundtrip);
            }
            u
        })
        .collect();
    parts.control = ExpressionControlFlow::try_new_in(
        parts.control.regions().values().copied().collect(),
        uses,
        &Definitions(parts.definitions.iter().map(|n| n.id).collect()),
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap();
    assert!(
        matches!(BoundExprSet::try_from_parts_in(parts,limits(),&mut work),Err(UnitError::Invalid {definition:Some(actual),message,..}) if actual==root && message.contains("temporal invocation shape"))
    );
}
